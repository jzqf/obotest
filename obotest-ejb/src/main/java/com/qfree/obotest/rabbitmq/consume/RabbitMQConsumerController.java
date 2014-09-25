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
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
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
		"RabbitMQConsumerHelperImageTestBean1", "RabbitMQConsumerHelperImageTestBean2",
		"RabbitMQConsumerHelperPassageTest1Bean1", "RabbitMQConsumerHelperPassageTest1Bean2" })
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Singleton
@LocalBean
public class RabbitMQConsumerController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerController.class);

	public enum RabbitMQConsumerControllerStates {
		STOPPED, RUNNING
	};

	public enum RabbitMQConsumerStates {
		STOPPED, RUNNING
	};

	public static final int NUM_RABBITMQ_CONSUMER_THREADS = 1;
	private static final long DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS = 4000;
	//	private static final long WAITING_LOOP_SLEEP_MS = 1000;
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
	private static final int MAX_MESSAGE_HANDLERS = 100;

	public static volatile RabbitMQConsumerControllerStates state = RabbitMQConsumerControllerStates.STOPPED;

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

	// NUM_RABBITMQ_CONSUMER_THREADS == 1:
	/*
	 * These are declared "volatile" because they are read in the method 
	 * getConsumerState(),and this method can be called from other threads, 
	 * such as from the RabbitMQProducerController singleton bean thread, as
	 * well as from the servlet that stops the consumer threads.
	 */
	private volatile RabbitMQConsumer rabbitMQConsumer = null;	//TODO TRY TO REMOVE DEPENDENCIES ON THIS AND THE OTHER "volatile" declarations!
	private volatile Thread rabbitMQConsumerThread = null;
	// NUM_RABBITMQ_CONSUMER_THREADS > 1:
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
	private volatile List<RabbitMQConsumer> rabbitMQConsumers = null;
	private volatile List<RabbitMQConsumerHelper> rabbitMQConsumerThreadImageEventSenders = null;
	private volatile List<Thread> rabbitMQConsumerThreads = null;
	//	public static final List<RabbitMQConsumer> rabbitMQConsumers =
	//			Collections.synchronizedList(new ArrayList<RabbitMQConsumer>());
	//	public static final List<RabbitMQConsumerHelper> rabbitMQConsumerThreadImageEventSenders =
	//			Collections.synchronizedList(new ArrayList<RabbitMQConsumerHelper>());
	//	public static final List<Thread> rabbitMQConsumerThreads =
	//			Collections.synchronizedList(new ArrayList<Thread>());

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
	 * Testing seems to indicate that qualifiers, e.g., @HelperBean1 & 
	 * @HelperBean2 don't work when EJBs are injected with @EJB, but they *do*
	 * work with injection via @Inject.
	 */
	//	@EJB
	@Inject
	@HelperBean1
	RabbitMQConsumerHelper messageConsumerHelperBean1;	// used by the first thread

	//	@EJB
	@Inject
	@HelperBean2
	RabbitMQConsumerHelper messageConsumerHelperBean2;	// used by the second thread

	// NUM_RABBITMQ_CONSUMER_THREADS == 1:
	@Lock(LockType.READ)
	public RabbitMQConsumerStates getConsumerState() {
		if (rabbitMQConsumerThread != null && rabbitMQConsumerThread.isAlive()) {
			if (rabbitMQConsumer != null) {
				return rabbitMQConsumer.getState();
			} else {
				// This should never happen. Am I being too careful?
				logger.error("rabbitMQConsumer is null, but its thread seems to be alive");
				return RabbitMQConsumerStates.STOPPED;
			}
		} else {
			return RabbitMQConsumerStates.STOPPED;
		}
	}
	// NUM_RABBITMQ_CONSUMER_THREADS > 1:
	@Lock(LockType.READ)
	public RabbitMQConsumerStates getConsumerState(int threadIndex) {
		if (threadIndex < NUM_RABBITMQ_CONSUMER_THREADS) {
			if (rabbitMQConsumerThreads.get(threadIndex) != null && rabbitMQConsumerThreads.get(threadIndex).isAlive()) {
				if (rabbitMQConsumers.get(threadIndex) != null) {
					return rabbitMQConsumers.get(threadIndex).getState();
				} else {
					// This should never happen. Am I being too careful?
					logger.error("rabbitMQConsumer {} is null, but its thread seems to be alive", threadIndex);
					return RabbitMQConsumerStates.STOPPED;
				}
			} else {
				return RabbitMQConsumerStates.STOPPED;
			}
		} else {
			logger.error("threadIndex = {}, but NUM_RABBITMQ_CONSUMER_THREADS = {}",
					threadIndex, NUM_RABBITMQ_CONSUMER_THREADS);
			return RabbitMQConsumerStates.STOPPED;	// simpler than throwing an exception :-)
		}
	}

	//	@Lock(LockType.WRITE)
	//	public boolean acquireMessageHandlerPermit() {
	//		/*
	//		 * Since the total number of permits is set to a large number, this 
	//		 * should, ideally, always succeed (since there is a reasonable upper 
	//		 * limit to how many threads the container will devote for receiving
	//		 * the CDI events. Assuming that the semaphore has 
	//		 * been constructed with a reasonably large number of maximum permits,
	//		 * if a permit is *not* acquired here, then the solution to this problem
	//		 * will probably *not* be to wait or increase the maximum number of 
	//		 * permits, but to look for a bug in the algorithm.
	//		 */
	//		boolean acquired = RabbitMQConsumerController.messageHandlerCounterSemaphore.tryAcquire();
	//		logger.debug("Message handler permit acquired. Number of active message handlers = {}",
	//				acquiredMessageHandlerPermits());
	//		return acquired;
	//	}

	//	@Lock(LockType.WRITE)
	//	public boolean releaseMessageHandlerPermit() {
	//		RabbitMQConsumerController.messageHandlerCounterSemaphore.release();
	//		logger.debug("Message handler permit released. Number of active message handlers = {}",
	//				acquiredMessageHandlerPermits());
	//		return true;
	//	}

	/**
	 * Returns the number of message handler permits currently acquired. This
	 * represents the number of message handlers threads that are currently
	 * processing messages consumed from a RabbitMQ broker. The threads are 
	 * started automatically by the Java EE application container as the target
	 * of CDI events that are fired by RabbitMQ message consumer threads.
	 * 
	 * @return the number of message handler permits currently acquired
	 */
	@Lock(LockType.READ)
	public int acquiredMessageHandlerPermits() {
		return MAX_MESSAGE_HANDLERS - RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits();
	}

	/*
	 * @Startup ensures that this method is called when the application starts 
	 * up.
	 */
	@PostConstruct
	void applicationStartup() {

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

		logger.info("Setting timer to trigger call to start() in {} ms...",
				DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS);
		@SuppressWarnings("unused")
		Timer timer =
				timerService.createSingleActionTimer(DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS, new TimerConfig());

		//		if (messageConsumerHelperBean1 == null) {
		//			logger.debug("messageConsumerHelperBean1 is null!");
		//		} else {
		//			logger.debug("messageConsumerHelperBean1 is not null");
		//		}
		//		if (messageConsumerHelperBean2 == null) {
		//			logger.debug("messageConsumerHelperBean2 is null!");
		//		} else {
		//			logger.debug("messageConsumerHelperBean2 is not null");
		//		}
		//		if (messageConsumerHelperBean1 == messageConsumerHelperBean2) {
		//			logger.debug("messageConsumerHelperBean1 and messageConsumerHelperBean2 are the same beans");
		//		} else {
		//			logger.debug("messageConsumerHelperBean1 and messageConsumerHelperBean2 are different beans");
		//		}

		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			/*
			 *  There is nothing to do here if we will start only a single 
			 *  thread because there is no need to use the lists that are
			 *  required for supporting multiple threads.
			 */
		} else {
			// Initialize lists with NUM_RABBITMQ_CONSUMER_THREADS null values each.
			rabbitMQConsumers = new ArrayList<>(Collections.nCopies(NUM_RABBITMQ_CONSUMER_THREADS,
					(RabbitMQConsumer) null));
			rabbitMQConsumerThreads = new ArrayList<>(Collections.nCopies(NUM_RABBITMQ_CONSUMER_THREADS, (Thread) null));
			//		for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
			//			rabbitMQConsumers.add(null);
			//			rabbitMQConsumerThreads.add(null);
			//		}

			if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
				// Initialize list rabbitMQConsumerThreadImageEventSenders with a 
				// different singleton session bean in each element.  These beans
				// will fire the CDI events from the RabbitMQ consumer threads that
				// are managed by the current singleton session bean
				rabbitMQConsumerThreadImageEventSenders = new ArrayList<>();
				rabbitMQConsumerThreadImageEventSenders.add(messageConsumerHelperBean1);
				if (NUM_RABBITMQ_CONSUMER_THREADS > 1) {
					rabbitMQConsumerThreadImageEventSenders.add(messageConsumerHelperBean2);
				}
			} else {
				logger.error(
						"{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
						NUM_RABBITMQ_CONSUMER_THREADS);
			}
		}

	}

	/*
	 * @Timout is used to implement a programmatic delay on application startup
	 * before the MessageMQ consumer thread is started. This method can also be
	 * called directly, i.e., not via the EJB timer service.
	*/
	@Timeout
	@Lock(LockType.WRITE)
	public void start() {
		logger.info("Request received to start RabbitMQ consumer thread");
		/*
		 * This test avoids starting the producer thread(s) when we know they
		 * will not function correctly.
		 */
		if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
			RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;
			logger.debug("Calling heartBeat()...");
			this.heartBeat();	// will start consumer thread(s), if necessary
		} else {
			logger.error("{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
					NUM_RABBITMQ_CONSUMER_THREADS);
		}
	}

	@Schedule(second = "*/4", minute = "*", hour = "*")
	@Lock(LockType.WRITE)
	public void heartBeat() {

		//		logger.debug("this.getState() = {}", this.getState());
		//		logger.debug("rabbitMQConsumerThreads.size() = {}", rabbitMQConsumerThreads.size());
		//		for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
		//			if (rabbitMQConsumerThreads.get(threadIndex) == null) {
		//				logger.info("rabbitMQConsumerThreads.get({}) is null", threadIndex);
		//			} else {
		//				logger.info("rabbitMQConsumerThreads.get({}) is not null", threadIndex);
		//			}
		//			if (rabbitMQConsumers.get(threadIndex) == null) {
		//				logger.info("rabbitMQConsumers.get({}) is null", threadIndex);
		//			} else {
		//				logger.info("rabbitMQConsumers.get({}) is not null", threadIndex);
		//			}
		//		}

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

					//					if (messageConsumerHelperBean1 == null) {
					//						logger.debug("messageConsumerHelperBean1 is null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					//					} else {
					//						logger.debug("messageConsumerHelperBean1 not null");
					//					}

					logger.info("Starting RabbitMQ consumer thread...");
					rabbitMQConsumer = new RabbitMQConsumer(messageConsumerHelperBean1);
					rabbitMQConsumerThread = threadFactory.newThread(rabbitMQConsumer);
					rabbitMQConsumerThread.start();

				} else {
					logger.trace("RabbitMQ consumer thread is already running");
				}
			} else {
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

						rabbitMQConsumers.set(threadIndex,
								new RabbitMQConsumer(rabbitMQConsumerThreadImageEventSenders.get(threadIndex)));
						rabbitMQConsumerThreads.set(threadIndex,
								threadFactory.newThread(rabbitMQConsumers.get(threadIndex)));
						rabbitMQConsumerThreads.get(threadIndex).start();

					} else {
						logger.trace("RabbitMQ consumer thread {} is already running", threadIndex);
					}
				}
			}

		}

	}

	@Lock(LockType.WRITE)
	public void stop() {
		logger.info("Request received to stop RabbitMQ consumer thread(s)");

		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;

		/*
		 * Signal the RabbitMQ consumer thread(s) so they can check the state
		 * set in this thread to see if they should self-terminate.
		 * 
		 * I have commented out this code to avoid the problem where the target
		 * thread may be in the act of processing a message, in which case the 
		 * interrupt will cause the thread to abort the processing, i.e., it
		 * won't be necessarily blocked waiting to receive the next message, 
		 * which was the original idea of how this interrupt was to be used; 
		 * hence, the message will probably we be lost unless some very fancy 
		 * book keeping is done.
		 */
		//		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
		//			if (rabbitMQConsumerThread != null && rabbitMQConsumerThread.isAlive()) {
		//				logger.debug("Interrupting the RabbitMQ consumer thread...");
		//				rabbitMQConsumerThread.interrupt();
		//			}
		//		} else {
		//			for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
		//				if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
		//					if (rabbitMQConsumerThreads.get(threadIndex) != null
		//							&& rabbitMQConsumerThreads.get(threadIndex).isAlive()) {
		//						logger.debug("Interrupting RabbitMQ consumer thread {}...", threadIndex);
		//						rabbitMQConsumerThreads.get(threadIndex).interrupt();
		//					}
		//				} else {
		//					logger.error(
		//							"{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
		//							NUM_RABBITMQ_CONSUMER_THREADS);
		//				}
		//			}
		//		}

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
		logger.info("Stopping the RabbitMQ consumer threads...");
		stopConsumerThreadsAndWaitForTermination();

		logger.info("RabbitMQ consumer controller will now be destroyed by the container");
	}

	/**
	 * Stops the RabbitMQconsumer thread(s) and then wait for it(them) to 
	 * terminate.
	 */
	@Lock(LockType.WRITE)
	public void stopConsumerThreadsAndWaitForTermination() {

		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			if (rabbitMQConsumerThread != null) {
				stop();	// call repeatedly, just in case
				logger.debug("Waiting for RabbitMQ consumer thread to terminate...");
				try {
					//TODO Make this 30000 ms a configurable parameter or a final static variable
					rabbitMQConsumerThread.join(30000);	// Wait maximum 30 seconds
				} catch (InterruptedException e) {
				}
			}
		} else {
			// TODO This is slightly more efficient and a little clearer. Use it when I have time to test it.
			//			for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
			for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
				if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
					if (rabbitMQConsumerThreads.get(threadIndex) != null) {
						stop();	// call repeatedly, just in case
						logger.debug("Waiting for RabbitMQ consumer thread {} to terminate...", threadIndex);
						try {
							//TODO Make this 30000 ms a configurable parameter or a final static variable
							rabbitMQConsumerThreads.get(threadIndex).join(30000);	// Wait maximum 30 seconds
						} catch (InterruptedException e) {
						}
					}
				} else {
					logger.error(
							"{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
							NUM_RABBITMQ_CONSUMER_THREADS);
				}
			}
		}

		// Another way of doing this that checks the "consumer state" instead
		// of checking directly that the thread(s) has(have) terminated.:

		//		long loopTime = 0;
		//		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
		//			while (this.getConsumerState() != RabbitMQConsumerStates.STOPPED) {
		//				stop();	// call repeatedly, just in case
		//				logger.debug("Waiting for RabbitMQ consumer thread to quit...");
		//				loopTime += WAITING_LOOP_SLEEP_MS;
		//				try {
		//					Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//				} catch (InterruptedException e) {
		//				}
		//				// Wait maximum 60 seconds.
		//				if (loopTime >= 60000) {
		//					logger.debug("Timeout waiting for RabbitMQ consumer thread to quit");
		//					break;
		//				}
		//			}
		//		} else {
		//			for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
		//				while (this.getConsumerState(threadIndex) != RabbitMQConsumerStates.STOPPED) {
		//					stop();	// call repeatedly, just in case
		//					logger.debug("Waiting for RabbitMQ consumer thread {} to quit...", threadIndex);
		//					loopTime += WAITING_LOOP_SLEEP_MS;
		//					try {
		//						Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//					} catch (InterruptedException e) {
		//					}
		//					// Wait maximum 60 seconds.
		//					if (loopTime >= 60000) {
		//						logger.debug("Timeout waiting for RabbitMQ consumer thread to quit");
		//						break;
		//					}
		//				}
		//			}
		//		}

	}
}
