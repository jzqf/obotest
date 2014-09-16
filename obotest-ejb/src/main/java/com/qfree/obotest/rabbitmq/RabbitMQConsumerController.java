package com.qfree.obotest.rabbitmq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

import com.qfree.obotest.eventsender.HelperBean1;
import com.qfree.obotest.eventsender.HelperBean2;
import com.qfree.obotest.eventsender.MessageConsumerHelper;
import com.qfree.obotest.thread.DefaultUncaughtExceptionHandler;

/*
 * @Startup marks this bean for "eager initialization" during the application 
 * startup sequence.
 * 
 * @DependsOn is iportant here. It not only ensures that the singleton beans
 * that are listed have been initialized before this singleton's PostConstruct 
 * method is called. This is probably not important because those beans that 
 * are used needed in threads started from this bean are injected below; this
 * probably means that they will exist and be initialized when they are needed 
 * here. More important is that during application shutdown the container 
 * ensures that all singleton beans on with which this singleton has a DependsOn
 * relationship are still available during this singleton's PreDestroy method.
 * Testing has shown that if this @DependsOn annotation is not used, at least
 * one exception is thrown because one or nore of the dependent beans (the beans
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
 * Container-managed concurrency is the default concurrency mechanism for an EJB
 * container, but we set is explicitly here anyway.
 */
@Startup
@DependsOn({ "MessageConsumerHelperImageTestBean1", "MessageConsumerHelperImageTestBean2" })
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

	private static final int NUM_RABBITMQ_CONSUMER_THREADS = 2;
	private static final long DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS = 4000;
	//	private static final long WAITING_LOOP_SLEEP_MS = 1000;
	
	@Resource
	ManagedThreadFactory threadFactory;

	@Resource
	TimerService timerService;

	/*
	 * The qualifiers @HelperBean1 & @HelperBean2 are needed here because the 
	 * classes of both of the singleton EJB objects to be injected here 
	 * implement the MessageConsumerHelper interface. One of these classes is 
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
	MessageConsumerHelper messageConsumerHelperBean1;	// used by the first thread

	//	@EJB
	@Inject
	@HelperBean2
	MessageConsumerHelper messageConsumerHelperBean2;	// used by the second thread

	private volatile RabbitMQConsumerControllerStates state = RabbitMQConsumerControllerStates.STOPPED;

	// NUM_RABBITMQ_CONSUMER_THREADS == 1:
	private RabbitMQConsumer rabbitMQConsumer = null;
	private Thread rabbitMQConsumerThread = null;
	// NUM_RABBITMQ_CONSUMER_THREADS > 1:
	// These are parallel lists (arrays could also be used). There will be one
	// element in each list for each RabbitMQ consumer thread to be started from
	// this singleton session bean.
	List<RabbitMQConsumer> rabbitMQConsumers = null;
	List<MessageConsumerHelper> rabbitMQConsumerThreadImageEventSenders = null;
	List<Thread> rabbitMQConsumerThreads = null;

	@Lock(LockType.READ)
	public RabbitMQConsumerControllerStates getState() {
		return state;
	}

	@Lock(LockType.WRITE)
	public void setState(RabbitMQConsumerControllerStates state) {
		this.state = state;
	}

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

	/*
	 * @Startup ensures that this method is called when the application starts 
	 * up.
	 */
	@PostConstruct
	void applicationStartup() {

		logger.debug("Entering applicationStartup()...");

		/*
		 * If an uncaught exception occurs in a thread, the handler set here
		 * ensures that details of both the thread where it occurred as well as
		 * the exception itself are logged.
		 */
		Thread.setDefaultUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());

		logger.debug("Setting timer to trigger call to start() in {} ms...",
				DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS);
		@SuppressWarnings("unused")
		Timer timer =
				timerService.createSingleActionTimer(DELAY_BEFORE_STARTING_RABBITMQ_CONSUMER_MS, new TimerConfig());

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

		/*
		 * I have comment out this code, because GlassFish does not seem able
		 * to start a new thread using a ManagedThreadFactory object in a
		 * @PostConstruct method.
		 * 
		 * The GlassFish log contains an error like:
		 * 
		 *     java.lang.IllegalStateException: Module obotest is disabled
		 *     
		 * This behaviour seems to have been reported by others:
		 * 
		 * http://stackoverflow.com/questions/23900826/glassfish-4-using-concurrency-api-to-create-managed-threads
		 * http://stackoverflow.com/questions/20446682/can-i-start-a-managedthread-in-a-singleton-enterprise-java-bean
		 * https://issues.jboss.org/browse/WFLY-2343
		 */
		//		try {
		//
		//			//Create a new thread using the thread factory created above.
		//			Thread myThread = threadFactory.newThread(new Runnable() {
		//				@Override
		//				public void run() {
		//					//				logger.debug("Running a task using Managed thread ...");
		//					logger.debug("Running a task using Managed thread ...");
		//				}
		//			});
		//
		//			//Start executing the thread.
		//			myThread.start();
		//		} catch (Throwable e) {
		//			logger.debug("Oh oh, rats.");
		//			e.printStackTrace();
		//			throw e;
		//		}

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
		if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
			this.setState(RabbitMQConsumerControllerStates.RUNNING);
			logger.debug("Calling heartBeat()...");
			this.heartBeat();	// will start consumer thread(s), if necessary
		} else {
			logger.error("{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
					NUM_RABBITMQ_CONSUMER_THREADS);
		}
	}

	@Lock(LockType.WRITE)
	public void stop() {
		logger.info("Request received to stop RabbitMQ consumer thread(s)");

		this.setState(RabbitMQConsumerControllerStates.STOPPED);

		/*
		 * Signal the RabbitMQ consumer thread(s) so they can check the state
		 * set in this thread to see if they should self-terminate.
		 */
		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			if (rabbitMQConsumerThread != null && rabbitMQConsumerThread.isAlive()) {
				logger.debug("Interrupting the RabbitMQ consumer thread...");
				rabbitMQConsumerThread.interrupt();
			}
		} else {
			for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
				if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
					if (rabbitMQConsumerThreads.get(threadIndex) != null
							&& rabbitMQConsumerThreads.get(threadIndex).isAlive()) {
						logger.debug("Interrupting RabbitMQ consumer thread {}...", threadIndex);
						rabbitMQConsumerThreads.get(threadIndex).interrupt();
					}
				} else {
					logger.error(
							"{} RabbitMQ consumer threads are not supported.\nMaximum number of threads supported is 2",
							NUM_RABBITMQ_CONSUMER_THREADS);
				}
			}
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

		if (this.getState() == RabbitMQConsumerControllerStates.RUNNING) {
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
					rabbitMQConsumer = new RabbitMQConsumer(this, messageConsumerHelperBean1);
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
								new RabbitMQConsumer(this, rabbitMQConsumerThreadImageEventSenders.get(threadIndex)));
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

	@PreDestroy
	public void terminate() {
		logger.info("Shutting down...");

		//		logger.info("Start of terminate() method: messageConsumerHelperBean1 = {}", messageConsumerHelperBean1);
		//		logger.info("Start of terminate() method: messageConsumerHelperBean2 = {}", messageConsumerHelperBean2);

		this.stop();

		// Wait for the consumer thread(s) to terminate.
		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			if (rabbitMQConsumerThread != null) {
				logger.debug("Waiting for RabbitMQ consumer thread to terminate...");
				try {
					rabbitMQConsumerThread.join(30000);	// Wait maximum 30 seconds
				} catch (InterruptedException e) {
				}
			}
		} else {
			for (int threadIndex = 0; threadIndex < rabbitMQConsumerThreads.size(); threadIndex++) {
				if (NUM_RABBITMQ_CONSUMER_THREADS <= 2) {
					if (rabbitMQConsumerThreads.get(threadIndex) != null) {
						logger.debug("Waiting for RabbitMQ consumer thread {} to terminate...", threadIndex);
						try {
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

		//		logger.info("End of terminate() method: messageConsumerHelperBean1 = {}", messageConsumerHelperBean1);
		//		logger.info("End of terminate() method: messageConsumerHelperBean2 = {}", messageConsumerHelperBean2);

		//		long loopTime = 0;
		//		if (NUM_RABBITMQ_CONSUMER_THREADS == 1) {
		//			while (this.getConsumerState() != RabbitMQConsumerStates.STOPPED) {
		//				logger.debug("Waiting for RabbitMQ consumer thread to quit...");
		//				loopTime = +WAITING_LOOP_SLEEP_MS;
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
		//					logger.debug("Waiting for RabbitMQ consumer thread {} to quit...", threadIndex);
		//					loopTime = +WAITING_LOOP_SLEEP_MS;
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

		logger.info("RabbitMQ consumer controller will now be destroyed by the container");
	}
}
