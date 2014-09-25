package com.qfree.obotest.rabbitmq.produce;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerControllerStates;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerStates;

/*
 * @Startup marks this bean for "eager initialization" during the application 
 * startup sequence.
 * 
 * @DependsOn is important here. It ensures that:
 *
 *   1. The singleton beans that are listed have been initialized before this 
 *      singleton's PostConstruct method is called. 
 * 
 *   2. During application shutdown the container ensures that all singleton 
 *      beans on with which this singleton has a DependsOnrelationship are still
 *      available during this singleton's PreDestroy method.
 * 
 *   See class com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController
 *   for more details regarding the use of the @DependsOn annotation
 * 
 * Container-managed concurrency is the default concurrency mechanism for an EJB
 * container, but we set is explicitly here anyway.
 */
@Startup
@DependsOn({ "RabbitMQConsumerController",
		"RabbitMQProducerHelperPassageTest1Bean1", "RabbitMQProducerHelperPassageTest1Bean2" })
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Singleton
@LocalBean
public class RabbitMQProducerController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerController.class);

	public enum RabbitMQProducerControllerStates {
		STOPPED, RUNNING
	};

	public enum RabbitMQProducerStates {
		STOPPED, RUNNING
	};

	private static final int NUM_RABBITMQ_PRODUCER_THREADS = 1;
	private static final long DELAY_BEFORE_STARTING_RABBITMQ_PRODUCER_MS = 2000;
	private static final int PRODUCER_BLOCKING_QUEUE_LENGTH = 100;	//TODO Make this smaller?
	//	private static final long PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS = 10000;
	private static final long WAITING_LOOP_SLEEP_MS = 1000;
	
	/*
	 * This queue holds the RabbitMQ messages waiting to be sent to a RabbitMQ
	 * message broker. Messages can be entered into this queue using:
	 * 
	 *     boolean sent = send(byte[] bytes);
	 */
	public static final BlockingQueue<byte[]> messageBlockingQueue = new LinkedBlockingQueue<>(
			PRODUCER_BLOCKING_QUEUE_LENGTH);

	@Resource
	ManagedThreadFactory threadFactory;

	@Resource
	TimerService timerService;

	/*
	 * The qualifiers @HelperBean1 & @HelperBean2 are needed here because the 
	 * classes of both of the singleton EJB objects to be injected here 
	 * implement the RabbitMQProducerHelper interface. One of these classes is 
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
	RabbitMQProducerHelper messageProducerHelperBean1;	// used by the first thread

	//	@EJB
	@Inject
	@HelperBean2
	RabbitMQProducerHelper messageProducerHelperBean2;	// used by the second thread

	@Inject
	RabbitMQConsumerController rabbitMQConsumerController;	// used in @PreDestroy - eliminate if possible

	public static volatile RabbitMQProducerControllerStates state = RabbitMQProducerControllerStates.STOPPED;

	// NUM_RABBITMQ_PRODUCER_THREADS == 1:
	private RabbitMQProducer rabbitMQProducer = null;
	private Thread rabbitMQProducerThread = null;
	// NUM_RABBITMQ_PRODUCER_THREADS > 1:
	// These are parallel lists (arrays could also be used). There will be one
	// element in each list for each RabbitMQ producer thread to be started from
	// this singleton session bean.
	private final List<RabbitMQProducer> rabbitMQProducers = null;
	private final List<RabbitMQProducerHelper> rabbitMQProducerThreadImageEventSenders = null;
	private final List<Thread> rabbitMQProducerThreads = null;

	//	// NUM_RABBITMQ_PRODUCER_THREADS == 1:
	//	@Lock(LockType.READ)
	//	public RabbitMQProducerStates getProducerState() {
	//		if (rabbitMQProducerThread != null && rabbitMQProducerThread.isAlive()) {
	//			if (rabbitMQProducer != null) {
	//				return rabbitMQProducer.getState();
	//			} else {
	//				// This should never happen. Am I being too careful?
	//				logger.error("rabbitMQProducer is null, but its thread seems to be alive");
	//				return RabbitMQProducerStates.STOPPED;
	//			}
	//		} else {
	//			return RabbitMQProducerStates.STOPPED;
	//		}
	//	}
	//
	//	// NUM_RABBITMQ_PRODUCER_THREADS > 1:
	//	@Lock(LockType.READ)
	//	public RabbitMQProducerStates getProducerState(int threadIndex) {
	//		if (threadIndex < NUM_RABBITMQ_PRODUCER_THREADS) {
	//			if (rabbitMQProducerThreads.get(threadIndex) != null && rabbitMQProducerThreads.get(threadIndex).isAlive()) {
	//				if (rabbitMQProducers.get(threadIndex) != null) {
	//					return rabbitMQProducers.get(threadIndex).getState();
	//				} else {
	//					// This should never happen. Am I being too careful?
	//					logger.error("rabbitMQProducer {} is null, but its thread seems to be alive", threadIndex);
	//					return RabbitMQProducerStates.STOPPED;
	//				}
	//			} else {
	//				return RabbitMQProducerStates.STOPPED;
	//			}
	//		} else {
	//			logger.error("threadIndex = {}, but NUM_RABBITMQ_PRODUCER_THREADS = {}",
	//					threadIndex, NUM_RABBITMQ_PRODUCER_THREADS);
	//			return RabbitMQProducerStates.STOPPED;	// simpler than throwing an exception :-)
	//		}
	//	}

	/*
	 * @Startup ensures that this method is called when the application starts 
	 * up.
	 */
	@PostConstruct
	void applicationStartup() {

		logger.info("Entering applicationStartup()...");

		logger.info("Setting timer to trigger call to start() in {} ms...",
				DELAY_BEFORE_STARTING_RABBITMQ_PRODUCER_MS);
		@SuppressWarnings("unused")
		Timer timer =
				timerService.createSingleActionTimer(DELAY_BEFORE_STARTING_RABBITMQ_PRODUCER_MS, new TimerConfig());

		//		if (messageProducerHelperBean1 == null) {
		//			logger.debug("messageProducerHelperBean1 is null!");
		//		} else {
		//			logger.debug("messageProducerHelperBean1 is not null");
		//		}
		//		if (messageProducerHelperBean2 == null) {
		//			logger.debug("messageProducerHelperBean2 is null!");
		//		} else {
		//			logger.debug("messageProducerHelperBean2 is not null");
		//		}
		//		if (messageProducerHelperBean1 == messageProducerHelperBean2) {
		//			logger.debug("messageProducerHelperBean1 and messageProducerHelperBean2 are the same beans");
		//		} else {
		//			logger.debug("messageProducerHelperBean1 and messageProducerHelperBean2 are different beans");
		//		}

		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
			/*
			 *  There is nothing to do here if we will start only a single 
			 *  thread because there is no need to use the lists that are
			 *  required for supporting multiple threads.
			 */
		} else {
			// Initialize lists with NUM_RABBITMQ_PRODUCER_THREADS null values each.
			for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_PRODUCER_THREADS; threadIndex++) {
				rabbitMQProducers.add(null);
				rabbitMQProducerThreads.add(null);
			}

			if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
				// Initialize list rabbitMQProducerThreadImageEventSenders with a 
				// different singleton session bean in each element.  These beans
				// will fire the CDI events from the RabbitMQ producer threads that
				// are managed by the current singleton session bean
				rabbitMQProducerThreadImageEventSenders.add(messageProducerHelperBean1);
				if (NUM_RABBITMQ_PRODUCER_THREADS > 1) {
					rabbitMQProducerThreadImageEventSenders.add(messageProducerHelperBean2);
				}
			} else {
				logger.error(
						"{} RabbitMQ producer threads are not supported.\nMaximum number of threads supported is 2",
						NUM_RABBITMQ_PRODUCER_THREADS);
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
	 * before the MessageMQ producer thread is started. This method can also be
	 * called directly, i.e., not via the EJB timer service.
	*/
	@Timeout
	@Lock(LockType.WRITE)
	public void start() {
		logger.info("Request received to start RabbitMQ producer thread");
		/*
		 * This test avoids starting the producer thread(s) when we know they
		 * will not function correctly.
		 */
		if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
			RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING;
			logger.debug("Calling heartBeat()...");
			this.heartBeat();	// will start producer thread(s), if necessary
		} else {
			logger.error("{} RabbitMQ producer threads are not supported.\nMaximum number of threads supported is 2",
					NUM_RABBITMQ_PRODUCER_THREADS);
		}
	}

	@Schedule(second = "*/4", minute = "*", hour = "*")
	@Lock(LockType.WRITE)
	public void heartBeat() {

		//		logger.debug("this.getState() = {}", this.getState());
		//		logger.debug("rabbitMQProducerThreads.size() = {}", rabbitMQProducerThreads.size());
		//		for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
		//			if (rabbitMQProducerThreads.get(threadIndex) == null) {
		//				logger.info("rabbitMQProducerThreads.get({}) is null", threadIndex);
		//			} else {
		//				logger.info("rabbitMQProducerThreads.get({}) is not null", threadIndex);
		//			}
		//			if (rabbitMQProducers.get(threadIndex) == null) {
		//				logger.info("rabbitMQProducers.get({}) is null", threadIndex);
		//			} else {
		//				logger.info("rabbitMQProducers.get({}) is not null", threadIndex);
		//			}
		//		}

		if (RabbitMQProducerController.state == RabbitMQProducerControllerStates.RUNNING) {
			if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
				logger.trace("Checking if RabbitMQ producer thread is running...");

				/*
				 * If the producer thread is not running, we start it here. If it was
				 * running earlier but has stopped in the meantime, then isAlive() 
				 * will return false; it is not allowed to restart a terminated thread,
				 * so we instantiate a new thread instead of attempting to restart it.
				 */
				if (rabbitMQProducerThread == null || !rabbitMQProducerThread.isAlive()) {

					//					if (messageProducerHelperBean1 == null) {
					//						logger.debug("messageProducerHelperBean1 is null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					//					} else {
					//						logger.debug("messageProducerHelperBean1 not null");
					//					}

					logger.info("Starting RabbitMQ producer thread...");
					rabbitMQProducer = new RabbitMQProducer(messageProducerHelperBean1);
					rabbitMQProducerThread = threadFactory.newThread(rabbitMQProducer);
					rabbitMQProducerThread.start();

				} else {
					logger.trace("RabbitMQ producer thread is already running");
				}
			} else {
				for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
					logger.trace("Checking if RabbitMQ producer thread {} is running...", threadIndex);

					/*
					 * If the producer thread is not running, we start it here. If it was
					 * running earlier but has stopped in the meantime, then isAlive() 
					 * will return false; it is not allowed to restart a terminated thread,
					 * so we instantiate a new thread instead of attempting to restart it.
					 */
					if (rabbitMQProducerThreads.get(threadIndex) == null
							|| !rabbitMQProducerThreads.get(threadIndex).isAlive()) {

						logger.info("Starting RabbitMQ producer thread {}...", threadIndex);

						rabbitMQProducers.set(threadIndex,
								new RabbitMQProducer(rabbitMQProducerThreadImageEventSenders.get(threadIndex)));
						rabbitMQProducerThreads.set(threadIndex,
								threadFactory.newThread(rabbitMQProducers.get(threadIndex)));
						rabbitMQProducerThreads.get(threadIndex).start();

					} else {
						logger.trace("RabbitMQ producer thread {} is already running", threadIndex);
					}
				}
			}

		}

	}

	//	@Lock(LockType.WRITE)
	//	public void stop() {
	//		logger.info("Request received to stop RabbitMQ producer thread(s)");
	//
	//		RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;
	//
	//		/*
	//		 * Signal the RabbitMQ producer thread(s) so they can check the state
	//		 * set in this thread to see if they should self-terminate. The 
	//		 * interrupt is necessary because they may be blocked polling for an 
	//		 * item to remove from the messageBlockingQueue blocking queue.
	//		 * 
	//		 * I have commented out this code to avoid the problem where the target
	//		 * thread may be in the act of processing a message, in which case the 
	//		 * interrupt will cause the thread to abort the processing, i.e., it
	//		 * won't be necessarily blocked somewhere where it can abort safely, 
	//		 * which was the original idea of how this interrupt was to be used; 
	//		 * hence, the message will probably we be lost unless some very fancy 
	//		 * book keeping is done.
	//		 */
	//		//		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
	//		//			if (rabbitMQProducerThread != null && rabbitMQProducerThread.isAlive()) {
	//		//				logger.debug("Interrupting the RabbitMQ producer thread...");
	//		//				rabbitMQProducerThread.interrupt();
	//		//			}
	//		//		} else {
	//		//			for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
	//		//				if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
	//		//					if (rabbitMQProducerThreads.get(threadIndex) != null
	//		//							&& rabbitMQProducerThreads.get(threadIndex).isAlive()) {
	//		//						logger.debug("Interrupting RabbitMQ producer thread {}...", threadIndex);
	//		//						rabbitMQProducerThreads.get(threadIndex).interrupt();
	//		//					}
	//		//				} else {
	//		//					logger.error(
	//		//							"{} RabbitMQ producer threads are not supported.\nMaximum number of threads supported is 2",
	//		//							NUM_RABBITMQ_PRODUCER_THREADS);
	//		//				}
	//		//			}
	//		//		}
	//
	//	}

	/*
	 * This method *must* be allowed to terminate. If it stays in an endless
	 * loop for any reason, it is not possible to shut down the GlassFish 
	 * server or to even undeploy this application.
	 */
	@PreDestroy
	public void terminate() {
		logger.info("Shutting down...");
		shutdown();
		logger.info("RabbitMQ producer controller will now be destroyed by the container");
	}

	/**
	 * Waits for all message handlers to finish processing their incoming 
	 * messages.
	 */
	@Lock(LockType.WRITE)
	public void shutdown() {
		/*
		 * Stop the RabbitMQ consumer thread(s) and then wait for them to 
		 * terminate.
		 * 
		 * The RabbitMQConsumerController bean is responsible for shutting 
		 * itself down in its @PreDestroy method, but the @PreDestroy method of
		 * the RabbitMQConsumerController bean will not run until *after* this
		 * RabbitMQProducerController bean is destroyed by the container (due to
		 * the dependency set in the @DependsOn annotation above). Therefore, 
		 * we force the consumer threads to stop here:
		 */
		logger.info("Waiting for the consumer threads to terminate...");
		stopConsumerThreadsAndWaitForTermination();

		//		logger.info("Executing rabbitMQConsumerController.setState(RabbitMQConsumerControllerStates.STOPPED);...");
		//		rabbitMQConsumerController.setState(RabbitMQConsumerControllerStates.STOPPED);
		//		logger.info("Executing rabbitMQConsumerController.stop()...");
		//		rabbitMQConsumerController.stop();// TODO MUST ALSO WAIT FOR THREAD TO BE CONFIRMED STOPPED
		//		long longSleep = 5000;
		//		logger.info("\n*************************************\nSleeping to see if this fixes the shutdown bug\n*************************************");
		//		try {
		//			Thread.sleep(longSleep);
		//		} catch (InterruptedException e) {
		//		}

			/*
			 * Now that the consumer thread(s) are terminated, there will be no new
			 * incoming messages to process, but some message handler threads may
			 * still be busy processing incoming messages that were received a 
			 * little earlier. We must wait for those message handler threads to
			 * finish their processing and place their outgoing message in the 
			 * outgoing message queue. 
			 */
			logger.info("Waiting for all handler threads to finish processing their incoming messages...");
			waitForIncomingMessageHandlerThreadsToFinish();	//TODO Check if this is thread-safe or if we need "volatile"

			//		logger.info("\n*************************************\nSleeping to see if this fixes the shutdown bug\n*************************************");
			//		try {
			//			Thread.sleep(longSleep);
			//		} catch (InterruptedException e) {
			//		}

			/* 
			 * Now that the consumer thread(s) are terminated and, in addition, all
			 * message handler threads have finished processing their coming 
			 * messages, the outgoing message queue can can be allowed to empty as
			 * the messages in this queue are published by the RabbitMQ producer 
			 * threads.
			 */
			logger.info("Waiting for the messageBlockingQueue queue to empty...");
			waitForRabbitMQProducerQueueToEmpty();

			//		logger.info("\n*************************************\nSleeping to see if this fixes the shutdown bug\n*************************************");
			//		try {
			//			Thread.sleep(longSleep);
			//		} catch (InterruptedException e) {
			//		}

			/*
			 * Now that the blocking queue that is is used to hold outgoing messages
			 * is empty, the producer thread(s) can be terminated.
			 */
			logger.info("Stopping the RabbitMQ producer threads...");
			stopProducerThreadsAndWaitForTermination();

			//		logger.info("\n*************************************\nSleeping to see if this fixes the shutdown bug\n*************************************");
			//		try {
			//			Thread.sleep(longSleep);
			//		} catch (InterruptedException e) {
			//		}

	}

	/**
	 * Stops the RabbitMQconsumer thread(s) and then wait for it(them) to 
	 * terminate.
	 */
	@Lock(LockType.WRITE)
	public void stopConsumerThreadsAndWaitForTermination() {

		/*
		 * TODO I see no good reason why this method cannot be written like 
		 * RabbitMQConsumerController.stopConsumerThreadsAndWaitForTermination()
		 * and call .join() for the thread(s) in question.  Then we can even
		 * get rid of the getConsumerState() & getConsumerState(threadIndex)
		 * methods!!!!!
		 */

		long loopTime = 0;
		if (RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			while (getConsumerState() != RabbitMQConsumerStates.STOPPED) {
				RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;
				logger.debug("Waiting for RabbitMQ consumer thread to quit...");
				loopTime += WAITING_LOOP_SLEEP_MS;
				try {
					Thread.sleep(WAITING_LOOP_SLEEP_MS);
				} catch (InterruptedException e) {
				}
				//TODO Make this 30000 ms a configurable parameter or a final static variable
				if (loopTime >= 30000) {
					logger.debug("Timeout waiting for RabbitMQ consumer thread to quit");
					break;
				}
			}
		} else {
			for (int threadIndex = 0; threadIndex < RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
				while (getConsumerState(threadIndex) != RabbitMQConsumerStates.STOPPED) {
					RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;
					logger.debug("Waiting for RabbitMQ consumer thread {} to quit...", threadIndex);
					loopTime += WAITING_LOOP_SLEEP_MS;
					try {
						Thread.sleep(WAITING_LOOP_SLEEP_MS);
					} catch (InterruptedException e) {
					}
					//TODO Make this 30000 ms a configurable parameter or a final static variable
					if (loopTime >= 30000) {
						logger.debug("Timeout waiting for RabbitMQ consumer thread to quit");
						break;
					}
				}
			}
		}

	}

	// NUM_RABBITMQ_CONSUMER_THREADS == 1:
	@Lock(LockType.READ)
	private RabbitMQConsumerStates getConsumerState() {
		if (RabbitMQConsumerController.rabbitMQConsumerThread != null
				&& RabbitMQConsumerController.rabbitMQConsumerThread.isAlive()) {
			if (RabbitMQConsumerController.rabbitMQConsumer != null) {
				return RabbitMQConsumerController.rabbitMQConsumer.getState();
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
	private RabbitMQConsumerStates getConsumerState(int threadIndex) {
		if (threadIndex < RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS) {
			if (RabbitMQConsumerController.rabbitMQConsumerThreads.get(threadIndex) != null
					&& RabbitMQConsumerController.rabbitMQConsumerThreads.get(threadIndex).isAlive()) {
				if (RabbitMQConsumerController.rabbitMQConsumers.get(threadIndex) != null) {
					return RabbitMQConsumerController.rabbitMQConsumers.get(threadIndex).getState();
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
					threadIndex, RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS);
			return RabbitMQConsumerStates.STOPPED;	// simpler than throwing an exception :-)
		}
	}

	/**
	 * Waits for all message handlers to finish processing their incoming 
	 * messages.
	 */
	@Lock(LockType.WRITE)
	public void waitForIncomingMessageHandlerThreadsToFinish() {

		long loopTime = 0;
		while (rabbitMQConsumerController.acquiredMessageHandlerPermits() > 0) {

			/*
			 * The start() method for the producer threads is called repeatedly
			 * in this loop. This is to ensure that these threads keep running
			 * while we wait for all message handlers to finish processing
			 * their incoming messages. There is no known reason
			 * why this should be be necessary - this is just defensive
			 * programming to handle the unlikely case where, from somewhere,
			 * a request come in to shut down these threads while we are waiting
			 * for the message handlers to finish their processing.
			 */
			//TODO This does not handle the case where we *want* to shut down without waiting for the message handlers to finish, but will this ever be needed?
			start();	// call repeatedly, just in case

			logger.debug("{} message handlers still processing incoming messages...",
					rabbitMQConsumerController.acquiredMessageHandlerPermits());

			loopTime += WAITING_LOOP_SLEEP_MS;
			try {
				Thread.sleep(WAITING_LOOP_SLEEP_MS);
			} catch (InterruptedException e) {
			}

			//TODO Make this 30000 ms a configurable parameter or a final static variable
			if (loopTime >= 30000) {
				logger.warn("Timeout waiting for all message handlers to finish processing their incoming messages");
				break;
			}

		}

		if (rabbitMQConsumerController.acquiredMessageHandlerPermits() == 0) {
			logger.debug("All message handlers have finished processing their incoming messages");
		} else {
			logger.warn(
					"{} message handlers did not finished processing their incoming messages. These messages will be lost!",
					rabbitMQConsumerController.acquiredMessageHandlerPermits());
		}

	}

	/**
	 * Waits for the outgoing message queue to become empty.
	 */
	@Lock(LockType.WRITE)
	public void waitForRabbitMQProducerQueueToEmpty() {

		long loopTime = 0;
		while (messageBlockingQueue.size() > 0) {

			/*
			 * The start() method for the producer threads is called repeatedly
			 * in this loop. This is to ensure that these threads keep running
			 * while we wait for the queue to empty. There is no known reason
			 * why this should be be necessary - this is just defensive
			 * programming to handle the unlikely case where, from somewhere,
			 * a request come in to shut down these threads while we are waiting
			 * for the queue to empty.
			 */
			//TODO This does not handle the case where we *want* to shut down without emptying the queue, but will this ever be needed?
			this.start();	// call repeatedly, just in case

			logger.debug("{} elements left in messageBlockingQueue. Waiting for it to empty...",
					messageBlockingQueue.size());

			loopTime += WAITING_LOOP_SLEEP_MS;
			try {
				Thread.sleep(WAITING_LOOP_SLEEP_MS);
			} catch (InterruptedException e) {
			}

			//TODO Make this 30000 ms a configurable parameter or a final static variable
			if (loopTime >= 30000) {
				logger.warn("Timeout waiting for messageBlockingQueue to empty");
				break;
			}

		}

		if (messageBlockingQueue.size() == 0) {
			logger.debug("The messageBlockingQueue queue is empty. The producer threads will new be stopped.");
		} else {
			logger.warn("{} elements left in messageBlockingQueue. These messages will be lost!",
					messageBlockingQueue.size());
		}

	}

	/**
	 * Stops the RabbitMQ producer thread(s) and then wait for it(them) to 
	 * terminate.
	 */
	@Lock(LockType.WRITE)
	public void stopProducerThreadsAndWaitForTermination() {

		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
			if (rabbitMQProducerThread != null) {
				RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;	// call repeatedly, just in case
				logger.debug("Waiting for RabbitMQ producer thread to terminate...");
				try {
					//TODO Make this 30000 ms a configurable parameter or a final static variable
					rabbitMQProducerThread.join(30000);	// Wait maximum 30 seconds
				} catch (InterruptedException e) {
				}
			}
		} else {
			// TODO This is slightly more efficient and a little clearer.
			//			for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_PRODUCER_THREADS; threadIndex++) {
			for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
				if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
					if (rabbitMQProducerThreads.get(threadIndex) != null) {
						RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;	// call repeatedly, just in case
						logger.debug("Waiting for RabbitMQ producer thread {} to terminate...", threadIndex);
						try {
							//TODO Make this 30000 ms a configurable parameter or a final static variable
							rabbitMQProducerThreads.get(threadIndex).join(30000);	// Wait maximum 30 seconds
						} catch (InterruptedException e) {
						}
					}
				} else {
					logger.error(
							"{} RabbitMQ producer threads are not supported.\nMaximum number of threads supported is 2",
							NUM_RABBITMQ_PRODUCER_THREADS);
				}
			}
		}

		// Another way of doing this that checks the "producer state" instead
		// of checking directly that the thread(s) has(have) terminated.:

		//		long loopTime = 0;
		//		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
		//			while (this.getProducerState() != RabbitMQProducerStates.STOPPED) {
		//				stop();	// call repeatedly, just in case
		//				logger.debug("Waiting for RabbitMQ producer thread to quit...");
		//				loopTime += WAITING_LOOP_SLEEP_MS;
		//				try {
		//					Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//				} catch (InterruptedException e) {
		//				}
		//				// Wait maximum 60 seconds.
		//				if (loopTime >= 60000) {
		//					logger.warn("Timeout waiting for RabbitMQ producer thread to quit");
		//					break;
		//				}
		//			}
		//		} else {
		//			for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
		//				while (this.getProducerState(threadIndex) != RabbitMQProducerStates.STOPPED) {
		//				stop();	// call repeatedly, just in case
		//					logger.debug("Waiting for RabbitMQ producer thread {} to quit...", threadIndex);
		//					loopTime += WAITING_LOOP_SLEEP_MS;
		//					try {
		//						Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//					} catch (InterruptedException e) {
		//					}
		//					// Wait maximum 60 seconds.
		//					if (loopTime >= 60000) {
		//						logger.warn("Timeout waiting for RabbitMQ producer thread to quit");
		//						break;
		//					}
		//				}
		//			}
		//		}

	}
}
