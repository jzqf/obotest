package com.qfree.obotest.rabbitmq.consume;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.DependsOn;
import javax.ejb.LocalBean;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TimerService;
import javax.enterprise.concurrent.ManagedThreadFactory;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.HelperBean1;
import com.qfree.obotest.rabbitmq.HelperBean2;
import com.qfree.obotest.thread.DefaultUncaughtExceptionHandler;

/*
 * @Startup marks this bean for "eager initialization" during the application 
 * startup sequence.
 * 
 * @DependsOn is important here. It not only ensures that the singleton beans
 * that are listed have been initialized before this singleton's PostConstruct 
 * method is called. This is probably not important because those beans that 
 * are used needed in threads started from this bean are injected below; this
 * probably means that they will exist and be initialized when they are needed 
 * here. More important is that during application shutdown the container 
 * ensures that all singleton beans on with which this singleton has a DependsOn
 * relationship are still available during this singleton's PreDestroy method.
 * Testing has shown that if this @DependsOn annotation is not used, at least
 * one exception is thrown because one or more of the dependent beans (the beans
 * that should be listed in the @DependsOn annotation) are destroyed early 
 * while the message consumer thread(s) is(are) shutting down. The exception 
 * that is thrown is:
 * 
 *     javax.ejb.EJBException: Attempt to invoke when container is in STOPPED
 * 
 * Unfortunately, *all* beans that can _potentially_ be injected here must be
 * listed in the @DependsOn annotation, even only two will ever be injected
 * during any one run of this application (the two beans listed in the 
 * <alternatives> element of beans.xml that are injected into 
 * messageConsumerHelperBean1 and messageConsumerHelperBean2 below)). But there 
 * is no way to specify just those beans here, other than hardwiring their names 
 * here. So to avoid the need to edit this annotation each time we change these
 * alternatives (which is possible, but the application must be re-compiled),
 * *all* beans that can potentially be injected here are listed in the 
 * @DependsOn annotation.
 * 
 * Note that just the ejb-names of the singleton classes are listed in the 
 * @DependsOn annotation, The ejb-name of a singleton class defaults to the 
 * unqualified name of the singleton session bean class. If these unqualified
 * names are not unique, it is necessary to specify unique names for the beans
 * using the "name" element of the @Singleton annotation and then use those bean
 * names here in the @DependsOn annotation.
 * 
 * Container-managed concurrency is the default concurrency mechanism for an EJB
 * container, but we set is explicitly here anyway.
 */
@Startup
@DependsOn({
		//"RabbitMQConsumerHelperImageTestBean1", "RabbitMQConsumerHelperImageTestBean2",
		"RabbitMQConsumerHelperPassageTest1Bean1", "RabbitMQConsumerHelperPassageTest1Bean2" })
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Singleton
@LocalBean
public class RabbitMQConsumerController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerController.class);

	public enum RabbitMQConsumerControllerStates {
		STOPPED, DISABLED, RUNNING
	};

	public enum AckAlgorithms {
		AFTER_RECEIVED, AFTER_PUBLISHED, AFTER_PUBLISHED_CONFIRMED, AFTER_PUBLISHED_TX
	};

	public static final AckAlgorithms ackAlgorithm = AckAlgorithms.AFTER_PUBLISHED_CONFIRMED;

	public static final int NUM_RABBITMQ_CONSUMER_THREADS = 2;
	private static final long DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS = 4000;
	private static final long MAX_WAIT_BEFORE_THREAD_TERMINATION_MS = 30000;

	/*
	 * If true, RabbitMQMsgEnvelope objects will be passed to the message
	 * handlers by firing a "CDI event"; otherwise, a direct call to the 
	 * message handler will be made.
	 */
	public static final boolean MESSAGE_HANDLER_USE_CDI_EVENTS = false;
	/*
	 * If true, the message handlers will be invoked using an asynchronous
	 * mechanism. This mechanism can be either a direct call or by firing a CDI
	 * event, according to the setting of MESSAGE_HANDLER_USE_CDI_EVENTS.
	 */
	public static final boolean MESSAGE_HANDLER_ASYNCHRONOUS_CALLS = true;

	/*
	 * This is the maximum number of message handler threads that are allowed to
	 * run simultaneously. This is set to a sufficiently large number that this
	 * limit should never be reached because there is no desire or attempt to
	 * limit the number of such threads (the application container will manage
	 * this automatically). Rather, the counting semaphore 
	 * messageHandlerCounterSemaphore is used to monitor the number of such 
	 * threads and any Semaphore object needs to be initialized with the 
	 * maximum number of permits that it will allow to be acquired.
	 */
	public static final int MAX_MESSAGE_HANDLERS = 100;
	/*
	 * This is the maximum number of CDI events that have been fired from one of
	 * the RabbitMQ consumer threads but which have not yet been acknowledged
	 * by a stateless session been that receives the event in one of its methods
	 * that is annotated with @Observes. The value set here should be larger 
	 * than the maximum number of message handler threads that should ever be 
	 * created. We should never reach this limit. It needs to be large enough so
	 * that we never block when attempting to acquire a permit.
	 */
	public static final int UNSERVICED_ASYNC_CALLS_MAX = 100;

	/*
	 * This is the size of the queue that holds RabbitMQMsgAck objects sent
	 * back to the consumer threads to ack/nack messages that have been 
	 * processed in other threads, either successfully or unsuccessfully.
	 */
	public static final int ACKNOWLEDGEMENT_QUEUE_LENGTH = 2000;	//TODO Optimize queue size?

	/*
	 * This counting semaphore is used to count the number of asynchronous calls
	 * or the number of CDI events that have been fired from one of the RabbitMQ
	 * consumer threads but which have not yet being serviced by a stateless 
	 * session been that begins executing the call or receives the event in one 
	 * of its methods that is annotated with @Observes. This number is used to 
	 * throttle the RabbitMQ consumer threads so that we do not initiate too 
	 * many asynchronous calls or fire too many CDI events that cannot be 
	 * serviced in a timely fashion. This is done to try to strike a balance
	 * between the rate that RabbitMQ messages are consumed and the rate at 
	 * which they can be processed and then published back to another RabbitMQ
	 * exchanged. If the number of unserviced asynchronous call exceeds
	 * UNSERVICED_ASYNC_CALLS_HIGH_WATER, throttling will be invoked until
	 * the number of unserviced asynchronous call drops below
	 * UNSERVICED_ASYNC_CALLS_LOW_WATER. These limits are defined elsewhere.
	 */
	public static final Semaphore unacknowledgeCDIEventsCounterSemaphore = new Semaphore(UNSERVICED_ASYNC_CALLS_MAX);

	/*
	 * This counting semaphore is used to count the number of threads that are
	 * currently processing consumed (incoming) RabbitMQ messages. While it can
	 * be used to monitor the number of messages being processed at any time,
	 * it was introduced to wait during application shutdown or undeployment
	 * until all such threads have finished their work. This is necessary so
	 * that we do not loose any consumed messages when the application is shut
	 * down or the application is undeployed form the container (which is done
	 * whenever the application is *re*deployed).
	 */
	public static final Semaphore messageHandlerCounterSemaphore = new Semaphore(MAX_MESSAGE_HANDLERS);

	public static volatile RabbitMQConsumerControllerStates state = RabbitMQConsumerControllerStates.STOPPED;

	// This is for NUM_RABBITMQ_CONSUMER_THREADS == 1:
	/*
	 * These are declared "volatile" because they are read in the method 
	 * getConsumerState(),and this method can be called from other threads, 
	 * such as from the RabbitMQProducerController singleton bean thread, as
	 * well as from the servlet that stops the consumer threads.
	 */
	//TODO Make rabbitMQConsumerRunnable & rabbitMQConsumerThread non-static (will need rabbitMQConsumerRunnable getter)?
	public static volatile RabbitMQConsumerRunnable rabbitMQConsumerRunnable = null;
	public static volatile Thread rabbitMQConsumerThread = null;
	// This is for NUM_RABBITMQ_CONSUMER_THREADS > 1:
	/*
	 * These are parallel lists (arrays could also be used). There will be one 
	 * element in each list for each RabbitMQ consumer thread to be started from
	 * this singleton session bean.
	 * 
	 * These are declared "volatile" because they are read in the method 
	 * getConsumerState(int threadIndex),and this method can be called from 
	 * other threads, such as from the RabbitMQProducerController singleton bean
	 * thread, as well as from the servlet that stops the consumer threads.
	 */
	//TODO Make rabbitMQConsumerRunnables, rabbitMQConsumerThreadHelpers & rabbitMQConsumerThreads non-static (will need rabbitMQConsumerRunnables & rabbitMQConsumerThreads getter)?
	public static final List<RabbitMQConsumerRunnable> rabbitMQConsumerRunnables =
			Collections.synchronizedList(new ArrayList<RabbitMQConsumerRunnable>());
	public static final List<RabbitMQConsumerHelper> rabbitMQConsumerThreadHelpers =
			Collections.synchronizedList(new ArrayList<RabbitMQConsumerHelper>());
	public static final List<Thread> rabbitMQConsumerThreads =
			Collections.synchronizedList(new ArrayList<Thread>());

	@Resource
	ManagedThreadFactory threadFactory;

	@Resource
	TimerService timerService;

	/*
	 * The qualifiers @HelperBean1 & @HelperBean2 are needed here because the 
	 * classes of both of the singleton EJB objects to be injected here 
	 * implement the RabbitMQConsumerHelper interface. One of these classes is 
	 * annotated with the qualifier @HelperBean1 and the other is annotated with
	 * the qualifier @HelperBean2. This will ensure that each thread will get 
	 * its own singleton helper EJB. This will reduce contention over sharing 
	 * the *same* singleton between both/all threads.
	 * 
	 * The qualifiers, e.g., @HelperBean1 & @HelperBean2 don't work when EJBs 
	 * are injected with @EJB, but they *do* work with injection via @Inject.
	 * This must be because the CDI framework is used.
	 */
	//	@EJB
	@Inject
	@HelperBean1
	RabbitMQConsumerHelper messageConsumerHelperBean1;	// used by the first thread

	//	@EJB
	@Inject
	@HelperBean2
	RabbitMQConsumerHelper messageConsumerHelperBean2;	// used by the second thread

	/*
	 * @Startup ensures that this method is called when the application starts 
	 * up.
	 */
	@PostConstruct
	public void applicationStartup() {

		logger.info("Entering applicationStartup()...");

		/*
		 * If an uncaught exception occurs in a thread, the handler set here
		 * ensures that details of both the thread where it occurred as well as
		 * the exception itself are logged.
		 * 
		 * This handler is set here because currently, this startup singleton
		 * session bean is the first loaded. RabbitMQProducerController is 
		 * loaded after this bean because this bean is listed as a dependency
		 * of RabbitMQProducerController in the @DependsOn annotation in that
		 * class.
		 */
		Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());

		//		/*
		//		 * If this timer is used, uncomment the @Timeout annotation below;
		//		 * otherwise, there are java.lang.NullPointerException exceptions
		//		 * thrown in GlassFish's server.log.
		//		 */
		//		logger.info("Setting timer to trigger call to start() in {} ms...",
		//				DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS);
		//		@SuppressWarnings("unused")
		//		Timer timer =
		//				timerService.createSingleActionTimer(DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS, new TimerConfig());

		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			/*
			 *  There is nothing to do here if we will start only a single 
			 *  thread because there is no need to use the lists that are
			 *  required for supporting multiple threads.
			 */
		} else {
			// Initialize lists with NUM_RABBITMQ_CONSUMER_THREADS null values each.
			synchronized (rabbitMQConsumerRunnables) {
				for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
					rabbitMQConsumerRunnables.add(null);
					rabbitMQConsumerThreads.add(null);
				}
			}

			if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
				rabbitMQConsumerThreadHelpers.add(messageConsumerHelperBean1);
				if (NUM_RABBITMQ_CONSUMER_THREADS > 1) {
					rabbitMQConsumerThreadHelpers.add(messageConsumerHelperBean2);
				}
			} else {
				logger.error(
						"{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
						NUM_RABBITMQ_CONSUMER_THREADS);
			}
		}

	}

	/**
	 * Starts the MessageMQ consumer thread(s).
	 * 
	 * This method is annotated with @Timout to implement a programmatic delay 
	 * on application startup before the MessageMQ consumer thread(s) is(are) 
	 * started. This method can also be called directly, i.e., not via the EJB 
	 * timer service.
	 */
	//	@Timeout
	@Lock(LockType.WRITE)
	public void start() {
		logger.info("Request received to start RabbitMQ consumer thread(s)");
		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;
		logger.info("Calling heartBeat()...");
		this.heartBeat();	// will start consumer thread(s), if necessary
	}

	/**
	 * "Disables" the MessageMQ consumer thread(s).
	 */
	@Lock(LockType.WRITE)
	public void disable() {
		logger.info("Request received to disable RabbitMQ consumer thread(s)");
		/*
		 * We only "disable" the consumer threads if they are currently running;
		 * If, instead, the threads are currently stopped, setting the state to
		 * DISABLED will not necessarily cause any problems, but it does not
		 * follow a proper state machine mechanism that only running threads 
		 * can be disabled.
		 */
		if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.RUNNING) {
			RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.DISABLED;
		} else {
			logger.warn("Attempt to disable consumer threads when current state is {}",
					RabbitMQConsumerController.state);
		}
	}

	/**
	 * "Enables" the MessageMQ consumer thread(s).
	 */
	@Lock(LockType.WRITE)
	public void enable() {
		logger.info("Request received to enable RabbitMQ consumer thread(s)");
		/*
		 * We only "enable" the consumer threads if they are currently disabled;
		 * If, instead, the threads are currently stopped, setting the state to
		 * RUNNING will start the threads, which is not the same as "enabling"
		 * the threads. "Enabling" stopped threads also does not follow a proper
		 * state machine mechanism that only disabled threads can be enabled.
		 */
		if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.DISABLED) {
			RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;
		} else {
			logger.warn("Attempt to enable consumer threads when current state is {}",
					RabbitMQConsumerController.state);
		}
	}

	@Schedule(second = "*/4", minute = "*", hour = "*")
	@Lock(LockType.WRITE)
	private void heartBeat() {

		/*
		 * If the consumer threads are currently *not* running, we could also 
		 * start them if the current state is DISABLED (i.e., not only if it is 
		 * RUNNING). This would be so that it is possible to start these threads
		 * in an initially DISABLE state. However, this application does not 
		 * currently require this functionality, so I have not enabled it here.
		 * Perhaps in the future this might be appropriate.
		 */
		//		if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.RUNNING
		//				|| RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.DISABLED) {
		if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.RUNNING) {
			if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
				logger.trace("Checking if RabbitMQ consumer thread is running...");

				/*
				 * If the consumer thread is not running, we start it here. If it was
				 * running earlier but has stopped in the meantime, then isAlive() 
				 * will return false; it is not allowed to restart a terminated thread,
				 * so we instantiate a new thread instead of attempting to restart it.
				 */
				if (rabbitMQConsumerThread == null || !rabbitMQConsumerThread.isAlive()) {

					logger.info("Starting RabbitMQ consumer thread...");
					rabbitMQConsumerRunnable = new RabbitMQConsumerRunnable(messageConsumerHelperBean1);
					rabbitMQConsumerThread = threadFactory.newThread(rabbitMQConsumerRunnable);
					rabbitMQConsumerThread.start();

				} else {
					logger.trace("RabbitMQ consumer thread is already running");
				}
			} else {
				synchronized (rabbitMQConsumerRunnables) {
					for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
						logger.trace("Checking if RabbitMQ consumer thread {} is running...", threadIndex);

						/*
						 * If the consumer thread is not running, we start it here. If it was
						 * running earlier but has stopped in the meantime, then isAlive() 
						 * will return false; it is not allowed to restart a terminated thread,
						 * so we instantiate a new thread instead of attempting to restart it.
						 */
						if (rabbitMQConsumerThreads.get(threadIndex) == null
								|| !rabbitMQConsumerThreads.get(threadIndex).isAlive()) {

							logger.info("Starting RabbitMQ consumer thread {}...", threadIndex);

							rabbitMQConsumerRunnables.set(threadIndex,
									new RabbitMQConsumerRunnable(rabbitMQConsumerThreadHelpers.get(threadIndex)));
							rabbitMQConsumerThreads.set(threadIndex,
									threadFactory.newThread(rabbitMQConsumerRunnables.get(threadIndex)));
							rabbitMQConsumerThreads.get(threadIndex).start();

						} else {
							logger.trace("RabbitMQ consumer thread {} is already running", threadIndex);
						}
					}
				}
			}
		}
	}

	/*
	 * This method *must* be allowed to terminate. If it stays in an endless
	 * loop for any reason, it is not possible to shut down the GlassFish 
	 * server or to even undeploy this application.
	 */
	@PreDestroy
	public void terminate() {
		logger.info("Shutting down...");

		/*
		 * Stop the RabbitMQ consumer thread(s) and then wait for them to 
		 * terminate.
		 */
		logger.info("Stopping the RabbitMQ consumer threads and waiting for them to terminate...");
		stopConsumerThreadsAndWaitForTermination();

		logger.info("RabbitMQ consumer controller will now be destroyed by the container");
	}

	/**
	 * Stops the RabbitMQconsumer thread(s) and then wait for it(them) to 
	 * terminate.
	 */
	@Lock(LockType.WRITE)
	private void stopConsumerThreadsAndWaitForTermination() {

		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			if (rabbitMQConsumerThread != null && rabbitMQConsumerThread.isAlive()) {
				RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;	// call repeatedly, just in case
				logger.info("Waiting for RabbitMQ consumer thread to terminate...");
				try {
					rabbitMQConsumerThread.join(MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
					logger.info("RabbitMQ consumer thread terminated");
				} catch (InterruptedException e) {
				}
			}
		} else {
			for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
				if (rabbitMQConsumerThreads.get(threadIndex) != null
						&& rabbitMQConsumerThreads.get(threadIndex).isAlive()) {
					RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;	// call repeatedly, just in case
					logger.info("Waiting for RabbitMQ consumer thread {} to terminate...", threadIndex);
					try {
						rabbitMQConsumerThreads.get(threadIndex).join(MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
						logger.info("RabbitMQ consumer thread {} terminated", threadIndex);
					} catch (InterruptedException e) {
					}
				}
			}
		}

		logger.info("Done");
	}
}
