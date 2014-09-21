package com.qfree.obotest.rabbitmq.produce;

import java.util.ArrayList;
import java.util.Collections;
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
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerStates;
import com.qfree.obotest.thread.DefaultUncaughtExceptionHandler;

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

	private static final int NUM_RABBITMQ_PRODUCER_THREADS = 2;
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
	BlockingQueue<byte[]> messageBlockingQueue = new LinkedBlockingQueue<>(PRODUCER_BLOCKING_QUEUE_LENGTH);

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
	RabbitMQConsumerController rabbitMQConsumerController;	// used in @PreDestroy

	private volatile RabbitMQProducerControllerStates state = RabbitMQProducerControllerStates.STOPPED;

	// NUM_RABBITMQ_PRODUCER_THREADS == 1:
	private RabbitMQProducer rabbitMQProducer = null;
	private Thread rabbitMQProducerThread = null;
	// NUM_RABBITMQ_PRODUCER_THREADS > 1:
	// These are parallel lists (arrays could also be used). There will be one
	// element in each list for each RabbitMQ producer thread to be started from
	// this singleton session bean.
	private List<RabbitMQProducer> rabbitMQProducers = null;
	private List<RabbitMQProducerHelper> rabbitMQProducerThreadImageEventSenders = null;
	private List<Thread> rabbitMQProducerThreads = null;

	@Lock(LockType.READ)
	public RabbitMQProducerControllerStates getState() {
		return state;
	}

	@Lock(LockType.WRITE)
	public void setState(RabbitMQProducerControllerStates state) {
		this.state = state;
	}

	// NUM_RABBITMQ_PRODUCER_THREADS == 1:
	@Lock(LockType.READ)
	public RabbitMQProducerStates getProducerState() {
		if (rabbitMQProducerThread != null && rabbitMQProducerThread.isAlive()) {
			if (rabbitMQProducer != null) {
				return rabbitMQProducer.getState();
			} else {
				// This should never happen. Am I being too careful?
				logger.error("rabbitMQProducer is null, but its thread seems to be alive");
				return RabbitMQProducerStates.STOPPED;
			}
		} else {
			return RabbitMQProducerStates.STOPPED;
		}
	}

	// NUM_RABBITMQ_PRODUCER_THREADS > 1:
	@Lock(LockType.READ)
	public RabbitMQProducerStates getProducerState(int threadIndex) {
		if (threadIndex < NUM_RABBITMQ_PRODUCER_THREADS) {
			if (rabbitMQProducerThreads.get(threadIndex) != null && rabbitMQProducerThreads.get(threadIndex).isAlive()) {
				if (rabbitMQProducers.get(threadIndex) != null) {
					return rabbitMQProducers.get(threadIndex).getState();
				} else {
					// This should never happen. Am I being too careful?
					logger.error("rabbitMQProducer {} is null, but its thread seems to be alive", threadIndex);
					return RabbitMQProducerStates.STOPPED;
				}
			} else {
				return RabbitMQProducerStates.STOPPED;
			}
		} else {
			logger.error("threadIndex = {}, but NUM_RABBITMQ_PRODUCER_THREADS = {}",
					threadIndex, NUM_RABBITMQ_PRODUCER_THREADS);
			return RabbitMQProducerStates.STOPPED;	// simpler than throwing an exception :-)
		}
	}

	public BlockingQueue<byte[]> getMessageBlockingQueue() {
		return messageBlockingQueue;
	}

	//TODO setMessageBlockingQueue is not needed and can be deleted.
	//	public void setMessageBlockingQueue(BlockingQueue<byte[]> messageBlockingQueue) {
	//		this.messageBlockingQueue = messageBlockingQueue;
	//	}

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
			rabbitMQProducers = new ArrayList<>(Collections.nCopies(NUM_RABBITMQ_PRODUCER_THREADS,
					(RabbitMQProducer) null));
			rabbitMQProducerThreads = new ArrayList<>(Collections.nCopies(NUM_RABBITMQ_PRODUCER_THREADS, (Thread) null));
			//		for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_PRODUCER_THREADS; threadIndex++) {
			//			rabbitMQProducers.add(null);
			//			rabbitMQProducerThreads.add(null);
			//		}

			if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
				// Initialize list rabbitMQProducerThreadImageEventSenders with a 
				// different singleton session bean in each element.  These beans
				// will fire the CDI events from the RabbitMQ producer threads that
				// are managed by the current singleton session bean
				rabbitMQProducerThreadImageEventSenders = new ArrayList<>();
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
		if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
			this.setState(RabbitMQProducerControllerStates.RUNNING);
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

		if (this.getState() == RabbitMQProducerControllerStates.RUNNING) {
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
					rabbitMQProducer = new RabbitMQProducer(this, messageProducerHelperBean1);
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
								new RabbitMQProducer(this, rabbitMQProducerThreadImageEventSenders.get(threadIndex)));
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

	//TODO Delete this method. It was moved to PassageTest1Handler.
	//	public boolean send(byte[] bytes) {
	//		/*    
	//		 * If the queue is not full this will enter the message into the queue 
	//		 * and then return "true". If the queue is full, this call will block 
	//		 * for up to a certain timeout period. If a queue entry becomes 
	//		 * available before this timeout period is reached, the message is 
	//		 * placed in the queue and "true" is returned; otherwise, "false" is 
	//		 * returned and the sender must deal with the failure.
	//		 * 
	//		 * TODO Should we deal with the failure here in send()? Look into this!
	//		 */
	//		logger.debug("Offering a message to the producer blocking queue [{} bytes]...", bytes.length);
	//		boolean success = false;
	//		try {
	//			success = messageBlockingQueue.offer(bytes, PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
	//		} catch (InterruptedException e) {
	//			//TODO Can we ensure that the message to be queued, ie.e., "bytes", is not lost?
	//			logger.debug(
	//					"\n********************" +
	//							"\nInterruptedException caught while waiting to offer a message to the blocking queue. " +
	//							"\nPerhaps a request has been made to stop the RabbitMQ producer thread(s)?" +
	//							"\nCan we ensure that the message to be queued, ie.e., \"bytes\", is not lost?" +
	//					"\n********************");
	//		}
	//		if (success) {
	//			logger.debug("Message successfully entered into producer blocking queue.");
	//		} else {
	//			//TODO Can we ensure that the message to be queued, ie.e., "bytes", is not lost?
	//			logger.warn("\n**********\nMessage not entered into producer blocking queue. The queue is full!\n**********");
	//		}
	//		return success;
	//	}

	@Lock(LockType.WRITE)
	public void stop() {
		logger.info("Request received to stop RabbitMQ producer thread(s)");

		this.setState(RabbitMQProducerControllerStates.STOPPED);

		/*
		 * Signal the RabbitMQ producer thread(s) so they can check the state
		 * set in this thread to see if they should self-terminate. The 
		 * interrupt is necessary because they may be blocked polling for an 
		 * item to remove from the messageBlockingQueue blocking queue.
		 */
		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
			if (rabbitMQProducerThread != null && rabbitMQProducerThread.isAlive()) {
				logger.debug("Interrupting the RabbitMQ producer thread...");
				rabbitMQProducerThread.interrupt();
			}
		} else {
			for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
				if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
					if (rabbitMQProducerThreads.get(threadIndex) != null
							&& rabbitMQProducerThreads.get(threadIndex).isAlive()) {
						logger.debug("Interrupting RabbitMQ producer thread {}...", threadIndex);
						rabbitMQProducerThreads.get(threadIndex).interrupt();
					}
				} else {
					logger.error(
							"{} RabbitMQ producer threads are not supported.\nMaximum number of threads supported is 2",
							NUM_RABBITMQ_PRODUCER_THREADS);
				}
			}
		}

	}

	@PreDestroy
	public void terminate() {
		logger.info("Shutting down...");

		/*
		 * Request that the RabbitMQ consumer thread(s) be stopped. This call
		 * is OK even if these threads have already been stopped.
		 */
		rabbitMQConsumerController.stop();

		/*
		 * The RabbitMQConsumerController bean is responsible for shutting 
		 * itself down in its @PreDestroy method, but Tte @PreDestroy method of
		 * the RabbitMQConsumerController bean will not run until *after* this
		 * RabbitMQProducerController bean is destroyed by the container (due to
		 * the dependency set in the @DependsOn annotation above). Therefore, it
		 * is not possible to check that the message consumer threads have 
		 * terminated by setting some sort "DESTROYED" state for the 
		 * @PreDestroy method of the RabbitMQConsumerController bean in. But it
		 * is possible to check that the message consumer threads have 
		 * terminated by calling one of:
		 * 
		 *     RabbitMQConsumerController.getConsumerState()
		 *     RabbitMQConsumerController.getConsumerState(int threadIndex)
		 * 
		 * so this is done here:
		 */
		long loopTime = 0;
		if (rabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			while (rabbitMQConsumerController.getConsumerState() != RabbitMQConsumerStates.STOPPED) {
				logger.debug("Waiting for RabbitMQ consumer thread to quit...");
				loopTime = +WAITING_LOOP_SLEEP_MS;
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
			logger.debug("The consumer thread has terminated");
		} else {
			for (int threadIndex = 0; threadIndex < rabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
				while (rabbitMQConsumerController.getConsumerState(threadIndex) != RabbitMQConsumerStates.STOPPED) {
					logger.debug("Waiting for RabbitMQ consumer thread {} to quit...", threadIndex);
					loopTime = +WAITING_LOOP_SLEEP_MS;
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
			logger.debug("The {} consumer threads have terminated",
					rabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS);
		}

		//		while (rabbitMQConsumerController.getState() != RabbitMQConsumerController.RabbitMQConsumerControllerStates.DESTROYED) {
		//			logger.debug("rabbitMQConsumerController.getState() = {}", rabbitMQConsumerController.getState());
		//			try {
		//				logger.debug("Sleeping {} ms waiting for the state of the {} bean to become {}",
		//						RabbitMQConsumerController.class.getSimpleName(),
		//						RabbitMQConsumerController.RabbitMQConsumerControllerStates.DESTROYED);
		//				Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//			} catch (InterruptedException e) {
		//			}
		//		}

		/* 
		 * Now that the consumer thread(s) are terminated, the 
		 * messageBlockingQueue can now empty. There will be no new incoming 
		 * messages to process, but those that are currently being processed
		 * must be given enough time to finish processing and place their 
		 * outgoing message in the queue. These remain messages will be sent
		 * by the producer thread(s), and eventually the queue should become
		 * empty. After this occurs, the producer thread(s) can be terminated, 
		 * and then this @PreDestroy method can be allowed to finish. But we 
		 * must ensure that the producer thread(s) is(are) running so that the
		 * blocking queue can be emptied. We check for that can start it(them), 
		 * if necessary.
		 */
		if (messageBlockingQueue.size() > 0) {
			logger.debug("Waiting for the messageBlockingQueue queue to empty...");
			//TODO Is this a good enough test here that the producer thread(s) really are running?
			if (this.getState() != RabbitMQProducerControllerStates.RUNNING) {
				this.start();
			}
		}
		loopTime = 0;
		while (messageBlockingQueue.size() > 0) {
			logger.debug("{} elements left in messageBlockingQueue. Waiting for it to empty...",
					messageBlockingQueue.size());
			loopTime = +WAITING_LOOP_SLEEP_MS;
			try {
				Thread.sleep(WAITING_LOOP_SLEEP_MS);
			} catch (InterruptedException e) {
			}
			//TODO Make this 30000 ms a configurable parameter or a final static variable
			if (loopTime >= 30000) {
				logger.debug("Timeout waiting for messageBlockingQueue to empty");
				break;
			}
		}
		if (messageBlockingQueue.size() == 0) {
			logger.debug("The messageBlockingQueue queue is empty. The producer threads will new be stopped.");
		} else {
			logger.warn("{} elements left in messageBlockingQueue. These messages will be lost!",
					messageBlockingQueue.size());
		}
		logger.debug("The producer threads will now be stopped.");

		this.stop();

		// Wait for the producer thread(s) to terminate.
		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
			if (rabbitMQProducerThread != null) {
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

		//		logger.info("End of terminate() method: messageProducerHelperBean1 = {}", messageProducerHelperBean1);
		//		logger.info("End of terminate() method: messageProducerHelperBean2 = {}", messageProducerHelperBean2);

		//		long loopTime = 0;
		//		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
		//			while (this.getProducerState() != RabbitMQProducerStates.STOPPED) {
		//				logger.debug("Waiting for RabbitMQ producer thread to quit...");
		//				loopTime = +WAITING_LOOP_SLEEP_MS;
		//				try {
		//					Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//				} catch (InterruptedException e) {
		//				}
		//				// Wait maximum 60 seconds.
		//				if (loopTime >= 60000) {
		//					logger.debug("Timeout waiting for RabbitMQ producer thread to quit");
		//					break;
		//				}
		//			}
		//		} else {
		//			for (int threadIndex = 0; threadIndex < rabbitMQProducerThreads.size(); threadIndex++) {
		//				while (this.getProducerState(threadIndex) != RabbitMQProducerStates.STOPPED) {
		//					logger.debug("Waiting for RabbitMQ producer thread {} to quit...", threadIndex);
		//					loopTime = +WAITING_LOOP_SLEEP_MS;
		//					try {
		//						Thread.sleep(WAITING_LOOP_SLEEP_MS);
		//					} catch (InterruptedException e) {
		//					}
		//					// Wait maximum 60 seconds.
		//					if (loopTime >= 60000) {
		//						logger.debug("Timeout waiting for RabbitMQ producer thread to quit");
		//						break;
		//					}
		//				}
		//			}
		//		}

		logger.info("RabbitMQ producer controller will now be destroyed by the container");
	}
}
