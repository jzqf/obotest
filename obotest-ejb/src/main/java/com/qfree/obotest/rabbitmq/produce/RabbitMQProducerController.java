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
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import javax.enterprise.concurrent.ManagedThreadFactory;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.HelperBean1;
import com.qfree.obotest.rabbitmq.HelperBean2;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerControllerStates;

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

	private static final int NUM_RABBITMQ_PRODUCER_THREADS = 2;
	private static final long DELAY_BEFORE_STARTING_RABBITMQ_PRODUCER_MS = 2000;
	public static final int PRODUCER_BLOCKING_QUEUE_LENGTH = 100;	//TODO Optimize queue size?
	//	private static final long PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS = 10000;
	private static final long WAITING_LOOP_SLEEP_MS = 1000;
	private static final long MAX_WAIT_BEFORE_THREAD_TERMINATION_MS = 30000;
	
	/*
	 * This queue holds the RabbitMQ messages waiting to be sent to a RabbitMQ
	 * message broker. Messages can be entered into this queue using:
	 * 
	 *     boolean sent = send(byte[] bytes);
	 */
	public static final BlockingQueue<RabbitMQMsgEnvelope> producerMsgQueue = new LinkedBlockingQueue<>(
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

	//	@Inject
	//	RabbitMQConsumerController rabbitMQConsumerController;	// used in @PreDestroy - eliminate if possible

	public static volatile RabbitMQProducerControllerStates state = RabbitMQProducerControllerStates.STOPPED;

	// NUM_RABBITMQ_PRODUCER_THREADS == 1:
	private RabbitMQProducerRunnable rabbitMQProducer = null;
	private Thread rabbitMQProducerThread = null;
	// NUM_RABBITMQ_PRODUCER_THREADS > 1:
	// These are parallel lists (arrays could also be used). There will be one
	// element in each list for each RabbitMQ producer thread to be started from
	// this singleton session bean.
	private final List<RabbitMQProducerRunnable> rabbitMQProducerRunnables =
			Collections.synchronizedList(new ArrayList<RabbitMQProducerRunnable>());
	private final List<RabbitMQProducerHelper> rabbitMQProducerThreadHelpers =
			Collections.synchronizedList(new ArrayList<RabbitMQProducerHelper>());
	private final List<Thread> rabbitMQProducerThreads =
			Collections.synchronizedList(new ArrayList<Thread>());

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
				rabbitMQProducerRunnables.add(null);
				rabbitMQProducerThreads.add(null);
			}

			if (NUM_RABBITMQ_PRODUCER_THREADS <= 2) {
				// Initialize list rabbitMQProducerThreadImageEventSenders with a 
				// different singleton session bean in each element.  These beans
				// will fire the CDI events from the RabbitMQ producer threads that
				// are managed by the current singleton session bean
				rabbitMQProducerThreadHelpers.add(messageProducerHelperBean1);
				if (NUM_RABBITMQ_PRODUCER_THREADS > 1) {
					rabbitMQProducerThreadHelpers.add(messageProducerHelperBean2);
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

	/**
	 * Starts the MessageMQ producer thread(s).
	 * 
	 * This method is annotated with @Timout to implement a programmatic delay 
	 * on application startup before the MessageMQ producer thread(s) is(are) 
	 * started. This method can also be called directly, i.e., not via the EJB 
	 * timer service.
	 */
	//	@Timeout
	@Lock(LockType.WRITE)
	public void start() {
		logger.info("Request received to start RabbitMQ producer thread(s)");
		RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING;
		logger.info("Calling heartBeat()...");
		this.heartBeat();	// will start producer thread(s), if necessary
	}

	@Schedule(second = "*/4", minute = "*", hour = "*")
	@Lock(LockType.WRITE)
	public void heartBeat() {

		//		logger.info("Running...");
		//		logger.info("RabbitMQProducerController.state = {}", RabbitMQProducerController.state);

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

					logger.info("Starting RabbitMQ producer thread...");
					rabbitMQProducer = new RabbitMQProducerRunnable(messageProducerHelperBean1);
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

						rabbitMQProducerRunnables.set(threadIndex,
								new RabbitMQProducerRunnable(rabbitMQProducerThreadHelpers.get(threadIndex)));
						rabbitMQProducerThreads.set(threadIndex,
								threadFactory.newThread(rabbitMQProducerRunnables.get(threadIndex)));
						rabbitMQProducerThreads.get(threadIndex).start();

					} else {
						logger.trace("RabbitMQ producer thread {} is already running", threadIndex);
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
		 * Disable the RabbitMQ consumer thread(s) so that we can monitor and
		 * confirm that all consumed messages are fully processed before we
		 * shut down. This confirmation and monitoring is done below.
		 */
		logger.info("Disabling the RabbitMQ consumer threads...");
		disableConsumerThreads();

		/*
		 * Now that the consumer thread(s) are disabled, no new CDI events 
		 * will be fired, but there may be outstanding CDI events that have not
		 * be received by a message handler by its @Observes method. We must 
		 * wait for those outstanding CDI events to be received and acknowledged
		 * by message handlers. 
		 */
		logger.info("Waiting for unacknowledged CDI events to be acknowledged by message handlers...");
		waitForAllCDIEventsToBeAcknowledged();

		/*
		 * Now that the consumer thread(s) are disabled and there are no 
		 * unacknowledged CDI events, there will be no new incoming messages to
		 * process, but some message handler threads may still be busy 
		 * processing incoming messages that were received a little earlier. We
		 * must wait for those message handler threads to  finish their 
		 * processing and place their outgoing message in the outgoing message 
		 * queue. 
		 */
		logger.info("Waiting for all handler threads to finish processing their incoming messages...");
		waitForIncomingMessageHandlerThreadsToFinish();

		/* 
		 * Now that the consumer thread(s) are disabled and, in addition, all
		 * message handler threads have finished processing their coming 
		 * messages, the outgoing message queue can can be allowed to empty as
		 * the messages in this queue are published by the RabbitMQ producer 
		 * threads.
		 */
		logger.info("Waiting for the producerMsgQueue queue to empty...");
		waitForRabbitMQProducerQueueToEmpty();
		
		/*
		 * Now that the blocking queue that is is used to hold outgoing messages
		 * is empty, the producer thread(s) can be terminated. *If* in 
		 * acknowledgement mode AFTER_PUBLISHED_CONFIRMED, these threads
		 * will not terminate until they each confirm that they have fully 
		 * processed their pending "PublisherConfirms" synchronized TreeMaps.
		 * This means that they have received all pending light-weight publisher
		 * confirms and have entered these results into the acknowledgement 
		 * queue for the appropriate consumer thread.
		 */
		logger.info("Stopping the RabbitMQ producer threads and waiting for them to terminate...");
		stopProducerThreadsAndWaitForTermination();

		/*
		 * Finally, stop the RabbitMQ consumer thread(s), which should be 
		 * disabled, and then wait for them to terminate. These threads will 
		 * not terminate until they each confirm that they have fully processed
		 * their acknowledgement queues. 
		 * 
		 * The RabbitMQConsumerController bean is responsible for shutting 
		 * itself down in its @PreDestroy method, but the @PreDestroy method of
		 * the RabbitMQConsumerController bean will not run until *after* this
		 * RabbitMQProducerController bean is destroyed by the container (due to
		 * the dependency set in the @DependsOn annotation above). Therefore, 
		 * we force the consumer threads to stop here:
		 */
		logger.info("Stopping the RabbitMQ consumer threads and waiting for them to terminate...");
		stopConsumerThreadsAndWaitForTermination();

	}

	/**
	 * Disables the RabbitMQconsumer thread(s).
	 */
	@Lock(LockType.WRITE)
	public void disableConsumerThreads() {
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
		logger.info("Done");
	}

	/**
	 * Waits for all unacknowledged CDI events, if any, to be acknowledged by 
	 * message handlers. These correspond to CDI events that have been fired,
	 * but not received by a message handler by its @Observes method.
	 */
	@Lock(LockType.WRITE)
	private void waitForAllCDIEventsToBeAcknowledged() {

		long loopTime = 0;
		while (unacknowledgedCDIEventPermits() > 0) {

			/*
			 * A request to start the producer threads is made repeatedly
			 * in this loop. This is to ensure that these threads keep running
			 * while we wait for the CDI events to be acknowledged. This may 
			 * have to be done once, but there is no known reason why it should
			 * be necessary to keep doing this in the loop - this is just 
			 * defensive programming to handle the unlikely case where, from 
			 * somewhere, a request come in to shut down these threads while we
			 * are waiting for the CDI events to be acknowledged. In order to 
			 * start these threads, it is important that this be done by 
			 * executing start(), and *not* by simply assigning the "RUNNING"
			 * state to the state attribute for the producer controller 
			 * singelton, i.e.,
			 * RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING;
			 * This will not work for starting the threads in this case because 
			 * heartBeat() will not run periodically while this method executes,
			 * since this method and heartBeat() are both methods of the same 
			 * class RabbitMQProducerController. To get around this issue, 
			 * start() calls heartBeat() explicitly for us.
			 */
			start();	// call repeatedly, just in case

			logger.info("{} outstanding CDI events waiting to be acknowledged...",
					unacknowledgedCDIEventPermits());

			loopTime += WAITING_LOOP_SLEEP_MS;
			try {
				Thread.sleep(WAITING_LOOP_SLEEP_MS);
			} catch (InterruptedException e) {
			}

			if (loopTime >= MAX_WAIT_BEFORE_THREAD_TERMINATION_MS) {
				logger.warn("Timeout waiting for all outstanding CDI events to be acknowledged");
				break;
			}

		}

		if (unacknowledgedCDIEventPermits() == 0) {
			logger.info("All outstanding CDI events have been acknowledged by amessage handler");
		} else {
			logger.warn(
					"{} CDI events are still outstanding and have not be acknowledged by a message handle. These messages may be lost!",
					acquiredMessageHandlerPermits());
		}

		logger.info("Done");
	}

	/**
	 * Waits for all message handlers to finish processing their incoming 
	 * messages.
	 */
	@Lock(LockType.WRITE)
	public void waitForIncomingMessageHandlerThreadsToFinish() {

		long loopTime = 0;
		while (acquiredMessageHandlerPermits() > 0) {

			/*
			 * A request to start the producer threads is made repeatedly
			 * in this loop. This is to ensure that these threads keep running
			 * while we wait for all message handlers to finish process the CDI
			 * events they received. This may have to be done once, but there is
			 * no known reason why it should be necessary to keep doing this in 
			 * the loop - this is just defensive programming to handle the 
			 * unlikely case where, from somewhere, a request come in to shut 
			 * down these threads while we are waiting for the message handlers
			 * to finish. In order to  start these threads, it is important that
			 * this be done by executing start(), and *not* by simply assigning
			 * the "RUNNING" state to the state attribute for the producer 
			 * controller singelton, i.e.,
			 * RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING;
			 * This will not work for starting the threads in this case because 
			 * heartBeat() will not run peridically while this method executes,
			 * since this method and heartBeat() are both methods of the same 
			 * class RabbitMQProducerController. To get around this issue, 
			 * start() calls heartBeat() explicitly for us.
			 */
			start();	// call repeatedly, just in case

			logger.info("{} message handlers still processing incoming messages...",
					acquiredMessageHandlerPermits());

			loopTime += WAITING_LOOP_SLEEP_MS;
			try {
				Thread.sleep(WAITING_LOOP_SLEEP_MS);
			} catch (InterruptedException e) {
			}

			if (loopTime >= MAX_WAIT_BEFORE_THREAD_TERMINATION_MS) {
				logger.warn("Timeout waiting for all message handlers to finish processing their incoming messages");
				break;
			}

		}

		if (acquiredMessageHandlerPermits() == 0) {
			logger.info("All message handlers have finished processing their incoming messages");
		} else {
			logger.warn(
					"{} message handlers did not finished processing their incoming messages. These messages will be lost!",
					acquiredMessageHandlerPermits());
		}

		logger.info("Done");
	}

	/**
	 * Waits for the outgoing message queue to become empty.
	 */
	@Lock(LockType.WRITE)
	public void waitForRabbitMQProducerQueueToEmpty() {

		long loopTime = 0;
		while (producerMsgQueue.size() > 0) {

			/*
			 * A request to start the producer threads is made repeatedly
			 * in this loop. This is to ensure that these threads keep running
			 * while we wait for the queue to empty. This may have to be done 
			 * once, but there is no known reason why it should be necessary to 
			 * keep doing this in the loop - this is just defensive
			 * programming to handle the unlikely case where, from somewhere,
			 * a request come in to shut down these threads while we are waiting
			 * for the queue to empty. In order to start these threads, it is 
			 * important that this be done by executing start(), and *not* by
			 * simply assigning the "RUNNING" state to the state attribute for
			 * the producer controller singelton, i.e.,
			 * RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING;
			 * This will not work for starting the threads in this case because 
			 * heartBeat() will not run peridically while this method executes,
			 * since this method and heartBeat() are both methods of the same 
			 * class RabbitMQProducerController. To get around this issue, 
			 * start() calls heartBeat() explicitly for us.
			 */
			start();	// call repeatedly, just in case

			logger.info("{} elements left in producerMsgQueue. Waiting for it to empty...",
					producerMsgQueue.size());

			loopTime += WAITING_LOOP_SLEEP_MS;
			try {
				Thread.sleep(WAITING_LOOP_SLEEP_MS);
			} catch (InterruptedException e) {
			}

			if (loopTime >= MAX_WAIT_BEFORE_THREAD_TERMINATION_MS) {
				logger.warn("Timeout waiting for producerMsgQueue to empty");
				break;
			}

		}

		if (producerMsgQueue.size() == 0) {
			logger.info("The producerMsgQueue queue is empty.");
		} else {
			logger.warn("{} elements left in producerMsgQueue. These messages will be lost!",
					producerMsgQueue.size());
		}

		logger.info("Done");
	}

	/**
	 * Stops the RabbitMQ producer thread(s) and then wait for it(them) to 
	 * terminate.
	 */
	@Lock(LockType.WRITE)
	public void stopProducerThreadsAndWaitForTermination() {

		if (NUM_RABBITMQ_PRODUCER_THREADS == 1) {
			if (rabbitMQProducerThread != null && rabbitMQProducerThread.isAlive()) {
				RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;	// call repeatedly, just in case
				logger.info("Waiting for RabbitMQ producer thread to terminate...");
				try {
					rabbitMQProducerThread.join(MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
					logger.info("RabbitMQ producer thread terminated or timed out after {} ms",
							MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
				} catch (InterruptedException e) {
				}
			}
		} else {
			for (int threadIndex = 0; threadIndex < NUM_RABBITMQ_PRODUCER_THREADS; threadIndex++) {
				if (rabbitMQProducerThreads.get(threadIndex) != null
						&& rabbitMQProducerThreads.get(threadIndex).isAlive()) {
					RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;	// call repeatedly, just in case
					logger.info("Waiting for RabbitMQ producer thread {} to terminate...", threadIndex);
					try {
						rabbitMQProducerThreads.get(threadIndex).join(MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
						logger.info("RabbitMQ producer thread {} terminated or timed out after {} ms", threadIndex,
								MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
					} catch (InterruptedException e) {
					}
				}
			}
		}

		logger.info("Done");
	}

	/**
	 * Stops the RabbitMQconsumer thread(s) and then wait for it(them) to 
	 * terminate.
	 */
	//TODO Consider eliminating this method and replace the call above to it 
	//     with a call to RabbitMQConsumerController.stopConsumerThreadsAndWaitForTermination
	//     since they are essentially the same. Remember that the RabbitMQConsumerController
	//     bean will be unavailable and its heartBeat() method will not run while that method
	//     executes here. So test carefully before committing this change.
	@Lock(LockType.WRITE)
	public void stopConsumerThreadsAndWaitForTermination() {

		if (RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {
			if (RabbitMQConsumerController.rabbitMQConsumerThread != null
					&& RabbitMQConsumerController.rabbitMQConsumerThread.isAlive()) {
				RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;	// call repeatedly, just in case
				logger.info("Waiting for RabbitMQ consumer thread to terminate...");
				try {
					RabbitMQConsumerController.rabbitMQConsumerThread.join(MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
					logger.info("RabbitMQ consumer thread terminated or timed out after {} ms",
							MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
				} catch (InterruptedException e) {
				}
			}
		} else {
			for (int threadIndex = 0; threadIndex < RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS; threadIndex++) {
				if (RabbitMQConsumerController.rabbitMQConsumerThreads.get(threadIndex) != null
						&& RabbitMQConsumerController.rabbitMQConsumerThreads.get(threadIndex).isAlive()) {
					RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;	// call repeatedly, just in case
					logger.info("Waiting for RabbitMQ consumer thread {} to terminate...", threadIndex);
					try {
						RabbitMQConsumerController.rabbitMQConsumerThreads.get(threadIndex).join(
								MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
						logger.info("RabbitMQ consumer thread {} terminated or timed out after {} ms", threadIndex,
								MAX_WAIT_BEFORE_THREAD_TERMINATION_MS);
					} catch (InterruptedException e) {
					}
				}
			}
		}

		logger.info("Done");
	}

	/**
	 * Returns the number of unacknowledged CDI events. These correspond to CDI
	 * events that have been fired, but not received by a message handler by its
	 * @Observes method.
	 * 
	 * @return the number of message handler permits currently acquired
	 */
	@Lock(LockType.READ)
	private int unacknowledgedCDIEventPermits() {
		return RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS -
				RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits();
	}

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
	private int acquiredMessageHandlerPermits() {
		return RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
				RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits();
	}

}
