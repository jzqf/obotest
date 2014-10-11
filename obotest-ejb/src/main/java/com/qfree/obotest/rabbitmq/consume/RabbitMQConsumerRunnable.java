package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerControllerStates;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

/*
 * This class is instantiated explicitly with "new"; hence, it cannot be managed
 * by the Java EE application container. Therefore, it makes no sense to use
 * the annotations, @Stateless, or @LocalBean here. For the same reason it is 
 * not possible to use dependency injection so the @EJB annotation cannot be 
 * used either.
 */
//@Stateless
//@LocalBean
public class RabbitMQConsumerRunnable implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConsumerRunnable.class);

	private static final int QUEUE_REMAINING_CAPACITY_LOW_WATER = RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH / 3;
	private static final int QUEUE_REMAINING_CAPACITY_HIGH_WATER = RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH * 2 / 3;
	private static final int UNACKNOWLEDGED_CDI_EVENTS_LOW_WATER = 2;	//TODO Optimize UNACKNOWLEDGED_CDI_EVENTS_LOW_WATER?
	private static final int UNACKNOWLEDGED_CDI_EVENTS_HIGH_WATER = 8;	//TODO Optimize UNACKNOWLEDGED_CDI_EVENTS_HIGH_WATER?

	private static final long SHORT_SLEEP_MS = 200;
	private static final long LONG_SLEEP_MS = 1000;
	//	private static final long THROTTLED_WAITING_LOOP_SLEEP_MS = SHORT_SLEEP_MS;
	//	private static final long DISABLED_WAITING_LOOP_SLEEP_MS = LONG_SLEEP_MS;

	/*
	 * To signal that these consumer threads should terminat, another thread
	 * will set:
	 * 
	 * RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.STOPPED 
	 * 
	 * If this thread detects this condition below it will terminate, but 
	 * only if certain conditions are met. If those conditions are not met,
	 * the thread will continue looping and then try again on the next trip
	 * through the loop. This is the maximum duration that thread 
	 * termination will be deferred in this way. After this duration, this
	 * thread will terminate anyway. This is a simple safety feature to 
	 * avoid getting stuck in an endless loop in the unlikely case that the
	 * conditions being tested for are *never* satisfied.
	 */
	private static final long MAX_WAIT_BEFORE_TERMINATION_MS = 60000;  // 60s

	/**
	 * When true, message consumption will be disabled. This variable is set 
	 * the the logical OR of other "throttled" variables that are set true or
	 * false based on specific conditions.
	 */
	public static volatile boolean throttled = false;
	/**
	 * Set to true when the number of free elements available in the 
	 * producerMsgQueue queue drops below QUEUE_REMAINING_CAPACITY_LOW_WATER. It
	 * will then stay false until the number of free elements subsequently rises
	 * above QUEUE_REMAINING_CAPACITY_HIGH_WATER.
	 */
	public static volatile boolean throttled_ProducerMsgQueue = false;
	/**
	 * Set to true when the number of free elements available in the 
	 * producerMsgQueue queue drops below QUEUE_REMAINING_CAPACITY_LOW_WATER. It
	 * will then stay false until the number of free elements subsequently rises
	 * above QUEUE_REMAINING_CAPACITY_HIGH_WATER.
	 */
	public static volatile boolean throttled_UnacknowledgedCDIEvents = false;

	private final UUID uuid = UUID.randomUUID();

	/**
	 * This list is used to hold elements removed from the acknowledgement 
	 * queue. Those elements that are meant for other consumer threads are 
	 * placed back into the queue.
	 */
	private final List<RabbitMQMsgAck> acknowldegementQueueElements = new ArrayList<>();

	private RabbitMQConsumerHelper messageConsumerHelper = null;

	/*
	 * This variable measures how long termination has been deferred because
	 * the conditions necessary for termination have not been met. This is
	 * used to implement a simple safety net to avoid getting caught in an
	 * endless loop when shutting down the consumer threads.
	 */
	private long terminationRequestedTime = 0;


	/*
	 * This constructor is necessary, since this is a stateless session bean,
	 * even though all instances of this bean used by the application are 
	 * created with the "new" operator and use a constructor with arguments.
	 */
	public RabbitMQConsumerRunnable() {
	}

	public RabbitMQConsumerRunnable(RabbitMQConsumerHelper messageConsumerHelper) {
		super();
		this.messageConsumerHelper = messageConsumerHelper;
	}

	@Override
	public void run() {

		logger.info("Starting RabbitMQ message consumer. UUID = {}...", uuid);

		/*
		 * Let the helper bean know the thread's UUID so that the helper can
		 * record this in a RabbitMQMsgAck object which can then be packaged in
		 * a CDI event together with the message data. It then 
		 * fires this CDI event, which is received in the @Observes method of a
		 * message handler.
		 */
		messageConsumerHelper.registerConsumerThreadUUID(uuid);

		try {
			messageConsumerHelper.openConnection();
			try {
				messageConsumerHelper.openChannel();
				try {

					/*
					 * This queue holds the RabbitMQ delivery tags and other details for 
					 * messages that are processed in other threads but which must be 
					 * acked/nacked in this consumer thread. A new queue is created each
					 * time a channel is openned because the delivery tags that are used
					 * to ack/nack the consumed messages are specific to the channel 
					 * used to consume the original messages.
					 */
					BlockingQueue<RabbitMQMsgAck> acknowledgementQueue = new LinkedBlockingQueue<>(
							RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH);

					messageConsumerHelper.setAcknowledgementQueue(acknowledgementQueue);

					messageConsumerHelper.configureConsumer();
					logger.info("Waiting for messages...");

					// These are for testing only. Delete after things work OK.
					final long NUM_MSGS_TO_CONSUME = 20;
					long msgs_consumed = 0;

					while (true) {

						if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.RUNNING) {

							// Update "throttled_ProducerMsgQueue", if necessary:
							int remainingCapacity = RabbitMQProducerController.producerMsgQueue.remainingCapacity();
							if (throttled_ProducerMsgQueue) {
								if (remainingCapacity >= QUEUE_REMAINING_CAPACITY_HIGH_WATER) {
									logger.info("Consumption throttling based on producer queue size is now *off*");
									throttled_ProducerMsgQueue = false;
								}
							} else {
								if (remainingCapacity <= QUEUE_REMAINING_CAPACITY_LOW_WATER) {
									logger.info("Consumption throttling based on producer queue size is now *on*");
									throttled_ProducerMsgQueue = true;
								}
							}

							// Update "throttled_UnacknowledgedCDIEvents", if necessary:
							int numUnacknowledgeCDIEvents = RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
									- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore
											.availablePermits();
							if (throttled_UnacknowledgedCDIEvents) {
								if (numUnacknowledgeCDIEvents <= UNACKNOWLEDGED_CDI_EVENTS_LOW_WATER) {
									logger.info("Consumption throttling based on unacknowldeged CDI events is now *off*");
									throttled_UnacknowledgedCDIEvents = false;
								}
							} else {
								if (numUnacknowledgeCDIEvents >= UNACKNOWLEDGED_CDI_EVENTS_HIGH_WATER) {
									logger.info("Consumption throttling based on unacknowldeged CDI events is now *on*");
									throttled_UnacknowledgedCDIEvents = true;
								}
							}

							throttled = throttled_ProducerMsgQueue || throttled_UnacknowledgedCDIEvents;

							/*
							 *	UE:  number of Unacknowledged CDI Events
							*	MH:  number of message handlers running
							*	PQ:  number of elements in the producer message queue
							*	AQ:  number of elements in the acknowledgement queue
							 */
							logger.debug(
									"UE={}, MH={}, PQ={}, AQ={}, PQ-Throt={}, UE-Throt={}, Throt={}",
									numUnacknowledgeCDIEvents,
									RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
											RabbitMQConsumerController.messageHandlerCounterSemaphore
													.availablePermits(),
									RabbitMQProducerController.producerMsgQueue.size(),
									RabbitMQConsumerController.acknowledgementQueue.size(),
									new Boolean(RabbitMQConsumerRunnable.throttled_ProducerMsgQueue),
									new Boolean(RabbitMQConsumerRunnable.throttled_UnacknowledgedCDIEvents),
									new Boolean(RabbitMQConsumerRunnable.throttled)
									);

							if (!throttled) {

								if (msgs_consumed < NUM_MSGS_TO_CONSUME) {
									msgs_consumed += 1;
									logger.info("\n\nAbout to consume message...\n\n");

									try {
										messageConsumerHelper.handleNextDelivery();
									} catch (InterruptedException e) {
										logger.warn("InterruptedException received.");
									} catch (InvalidProtocolBufferException e) {
										/*
										 * TODO Catch this exception in handleNextDelivery()? At any rate, the consumed
										 *      message should probably be rejected/dead-lettered, either there or here.
										 */
										logger.error("InvalidProtocolBufferException received.", e);
									} catch (IOException e) {
										/*
										 * TODO Catch this exception in handleNextDelivery()? At any rate, the consumed
										 *      message should probably be rejected/dead-lettered, either there or here.
										 */
										logger.error("IOException received.", e);
									} catch (ShutdownSignalException e) {
										// I'm not sure if/when this will occur.
										logger.info(
												"ShutdownSignalException received. The RabbitMQ connection will close.",
												e);
										break;
									} catch (ConsumerCancelledException e) {
										// I'm not sure if/when this will occur.
										logger.info(
												"ConsumerCancelledException received. The RabbitMQ connection will close.",
												e);
										break;
									} catch (Throwable e) {
										// I'm not sure if/when this will occur.
										// We log the exception, but do not terminate this thread.
										logger.error("Unexpected exception caught.", e);
									}

								} else {
									try {
										Thread.sleep(LONG_SLEEP_MS);
									} catch (InterruptedException e) {
									}
								}  // if(msgs_consumed<NUM_MSGS_TO_CONSUME)

							}
						}

						int numAcknowledgementsinQueue = 0;
						if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {
							numAcknowledgementsinQueue = acknowledgeMsgsInQueue(acknowledgementQueue);
						} else {
							numAcknowledgementsinQueue = 0;
						}

						/*
						 * Sleep for a short period, as appropriate. In normal operation
						 * while we are consuming messages, we will not pause here at all.
						 */
						sleepALittleBit(throttled, numAcknowledgementsinQueue);

						if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.STOPPED) {
							if (stoppingNowIsOK()) {
								break;
							}
						} else {
							terminationRequestedTime = 0;  // reset, if necessary
						}

					}

				} catch (IOException e) {
					logger.error(
							"Exception thrown setting up consumer object for RabbitMQ channel. This thread will terminate.",
							e);
				}

			} catch (IOException e) {
				logger.error(
						"Exception thrown setting up RabbitMQ channel for conusuming messages. This thread will terminate.",
						e);
			} finally {
				logger.info("Closing RabbitMQ channel...");
				messageConsumerHelper.closeChannel();
			}

		} catch (IOException e) {
			//TODO Log more details about the connection attempt here
			// What I write out and how will depend on how I specify the host details.
			// For example, I can specify the host, port, username, etc., separately, or
			// I can use a single AMQP URI.
			logger.error(
					"Exception thrown attempting to open connection to RabbitMQ broker. This thread will terminate.", e);
		} finally {
			/*
			 * If this thread is allowed to terminate without closing the 
			 * connection, there will be one or more unacknowledged 
			 * messages on the RabbitMQ broker, corresponding to the 
			 * value passed to "channel.basicQos(...)" above. The broker
			 * will return them to the queue after it detects the 
			 * connection is lost, but since the application container 
			 * may keep this thread around for some time so that it can
			 * be reused elsewhere, we need to explicitly clean up here.
			 * Since, the broker is supposed to close any open channels
			 * when the connection is closed it is not strictly 
			 * necessary to close the channel above, but good 
			 * programming does demand this.
			 */
			logger.info("Closing RabbitMQ connection...");
			try {
				messageConsumerHelper.closeConnection();
			} catch (IOException e) {
				logger.error("Exception caught closing RabbitMQ connection", e);
			}
		}

		logger.info("Thread exiting");
	}

	private int acknowledgeMsgsInQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue) {
		
		/*
		 * Process the elements in the acknowledgement queue for this thread.
		 */
		logger.info("Processing {} elements from the NEW THREAD-SPECIFIC acknowledgement queue...",
				acknowledgementQueue.size());
		while (acknowledgementQueue.size() > 0) {
			RabbitMQMsgAck rabbitMQMsgAck = acknowledgementQueue.poll();
			if (rabbitMQMsgAck != null) {
				logger.info("NEW THREAD-SPECIFIC acknowledgement queue. Delivery tag = {}",
						rabbitMQMsgAck.getDeliveryTag());
				//TODO Uncomment these lines!!!!!!!!!!:
				//				try {
				//					messageConsumerHelper.acknowledgeMsg(rabbitMQMsgAck);
				//				} catch (IOException e) {
				//					// This is very unlikely.
				//					logger.warn("Exception thrown acknowledging a RabbitMQ message.", e);
				//				}
			}
		}

		//OLD (DELETE):
		int numAcknowledgementsinQueue = RabbitMQConsumerController.acknowledgementQueue.size();
		if (numAcknowledgementsinQueue > 0) {
			/*
			 * Since the semaphore is defined with the same number of permits as
			 * consumer threads, acquiring a permit here should never block.
			 */
			if (RabbitMQConsumerController.acknowledgementQueueBusyCounterSemaphore.tryAcquire()) {

				/*
				 * Drain the acknowledgement queue to a collection and then process
				 * the elements of this collection. This means that for a short period, 
				 * the acknowledgement queue will be empty. That is why the test further
				 * below which checks if the acknowledgement queue is empty, only 
				 * performs this check if no permits are acquired for this semaphore.
				 * In this way it knows that no other threads are performing this 
				 * manipulation of the acknowledgement queue.
				 */
				while (true) {
					RabbitMQMsgAck rabbitMQMsgAck = RabbitMQConsumerController.acknowledgementQueue
							.poll();
					if (rabbitMQMsgAck != null) {
						acknowldegementQueueElements.add(rabbitMQMsgAck);
					} else {
						break;
					}
				}
				logger.info("Processing {} elements from the acknowledgement queue...",
						acknowldegementQueueElements.size());
				// Process the drained elements.
				for (RabbitMQMsgAck rabbitMQMsgAck : acknowldegementQueueElements) {
					// Check if the acknowledgement shall be handled by this thread.
					if (rabbitMQMsgAck.getConsumerThreadUUID().equals(this.uuid)) {
						/*
						 * Yes, the acknowledgement must be handled by this thread,
						 * so we do that here.
						 */
						try {
							messageConsumerHelper.acknowledgeMsg(rabbitMQMsgAck);
						} catch (IOException e) {
							// This is very unlikely.
							logger.warn("Exception thrown acknowledging a RabbitMQ message.", e);
						}
					} else {
						/*
						 * No, the acknowledgement must be handled by another consumer
						 * thread, so we return it to the queue.
						 */
						if (RabbitMQConsumerController.acknowledgementQueue.offer(rabbitMQMsgAck)) {
							logger.info(
									"RabbitMQMsgAck object offered *back* to acknowledgment queue: {}",
									rabbitMQMsgAck);
						} else {
							logger.warn("Acknowledgement queue is full. Message will be requeued when consumer threads are restarted.");
						}
					}
				}

				/*
				 * Remove all elements from acknowldegementQueueElements so that it 
				 * will be empty the next time we execute this block of code, rather
				 * than creating a new list each time.
				 */
				acknowldegementQueueElements.clear();

				RabbitMQConsumerController.acknowledgementQueueBusyCounterSemaphore.release();
			}
		}
		return numAcknowledgementsinQueue;
	}

	private void sleepALittleBit(boolean throttled, int numAcknowledgementsinQueue) {

		long sleepMs = SHORT_SLEEP_MS;
		if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.DISABLED) {
			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {

				if (numAcknowledgementsinQueue > 0) {
					sleepMs = 0;	// so we continue to acknowledge messages quickly
				} else {
					// We always want to acknowledge messages in a timely manner.
					sleepMs = SHORT_SLEEP_MS;
				}

			} else {
				/*
				 * If we do not acknowledge messages in the consumer threads and state=DISABLED, 
				 * then we can we can afford to sleep a little longer.
				 */
				sleepMs = LONG_SLEEP_MS;
			}
		} else if (!throttled) {
			/*
			 * This is the normal case where we are:
			 *   1. consuming messages (not DISABLED) and 
			 *   2. message consumption is not throtted.
			 */
			sleepMs = 0;
		}
		if (sleepMs > 0) {
			logger.info("Disabled. Sleeping for {} ms", sleepMs);
			try {
				Thread.sleep(sleepMs);
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * 
	 * @return true if it is OK for the current consumer thread to terminate.
	 */
	private boolean stoppingNowIsOK() {
		boolean stop = false;
		if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {

			if (terminationRequestedTime == 0) {
				// Record when thread termination was initially requested.
				terminationRequestedTime = System.currentTimeMillis();
			}
			// Check that we have not deferred thread termination too long.
			if (System.currentTimeMillis() < terminationRequestedTime + MAX_WAIT_BEFORE_TERMINATION_MS) {

				logger.info("Request to stop detected.");
				/*
				 * Before we allow the consumer threads to terminate, we try
				 * to ensue that the acknowledgement queue is empty and that
				 * it will *stay* empty. It is not a big deal if we let these
				 * threads terminate without performing a positive acknowledgement
				 * for a few messages because those messages will automatically
				 * be requeued after the connection to the RabbitMQ broker is
				 * closed. Of course,this will only be OK if the messages are 
				 * treated in an idempotent manner so that requeuing a message 
				 * will not cause problems. But it is possible (though unlikely),
				 * that there could still be one or more elements in the 
				 * acknowledgement queue corresponding to request the their 
				 * message be rejected (dead-lettered). It may not be appropriate
				 * to allow this messages to be automatically reueued. At any
				 * rate, it will be most beneficial if we can be sure that the
				 * acknowledgement queue is empty, and will stay that way, before
				 * we let the consumer threads terminate.
				 * 
				 * But to be more-or-less sure that an empty acknowledgement
				 * queue stays that way, we check *first* that:
				 * 
				 *   1. There are no unacknowledged CDI events.
				 *   2.	There are no message handlers still running
				 *   3. The producer message queue is empty.
				 * 
				 * These checks are a similar to those made in
				 * RabbitMQProducerController.shutdown(), but here I do not
				 * wait if the condition is not true and neither to I try to
				 * force the producer threads to run.
				 */
				if (unacknowledgedCDIEventPermits() == 0) {
					if (acquiredMessageHandlerPermits() == 0) {
						if (RabbitMQProducerController.producerMsgQueue.size() == 0) {
							/*
							 * As described above, we can only trust a check that the
							 * acknowledgement queue is empty if no other consumer threads
							 * are processing the acknowledgement queue. We test for that
							 * here. If another consumer queue is processing the 
							 * acknowledgement queue, we simply continue and then test
							 * again on the next trip through this loop. If the consumer
							 * threads are not first disabled, it may be difficult to stop 
							 * the consumer threads because the acknowledgment queue may never
							 * become empty. But this should not be looked upon as a problem. 
							 * All that is necessary is to disable the consumer threads,
							 * AND THEN stop them a short while later after the consumed 
							 * messages have been processed.
							 */
							if (acquiredAcknowledgementQueuePermits() == 0) {
								if (RabbitMQConsumerController.acknowledgementQueue.size() == 0) {
									logger.info("This thread will terminate.");
									stop = true;
								} else {
									logger.info(
											"Request to stop detected, but but there are still {} elements in the acknowledgement queue.",
											RabbitMQConsumerController.acknowledgementQueue.size());
								}
							} else {
								logger.info(
										"Request to stop detected, but there are {} other consumer thread(s) processing the acknowledgement queue.",
										acquiredAcknowledgementQueuePermits());
							}
						} else {
							logger.info(
									"Request to stop detected, but there are still {} elements in the producer queue.",
									RabbitMQProducerController.producerMsgQueue.size());
						}
					} else {
						logger.info(
								"Request to stop detected, but there are still {} message handlers running.",
								acquiredMessageHandlerPermits());
					}
				} else {
					logger.info(
							"Request to stop detected, but there are still {} unacknowledged CDI events.",
							unacknowledgedCDIEventPermits());
				}
			} else {
				logger.info(
						"Termination has been deferred longer than the maximum of {} ms. "
								+ "This thread will now terminate.",
						MAX_WAIT_BEFORE_TERMINATION_MS);
				stop = true;
			}
		} else {
			logger.info("Request to stop detected. This thread will terminate.");
			stop = true;
		}
		return stop;
	}

	/**
	 * Returns the number of acknowledgement queue permits currently acquired.
	 * This represents the number of consumer threads currently processing 
	 * the acknowledgement queue.
	 * 
	 * @return the number of acknowledgement queue permits currently acquired
	 */
	private int acquiredAcknowledgementQueuePermits() {
		return RabbitMQConsumerController.MAX_ACK_QUEUE_PERMITS -
				RabbitMQConsumerController.acknowledgementQueueBusyCounterSemaphore.availablePermits();
	}

	/**
	 * Returns the number of unacknowledged CDI events. These correspond to CDI
	 * events that have been fired, but not received by a message handler by its
	 * @Observes method.
	 * 
	 * @return the number of message handler permits currently acquired
	 */
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
	private int acquiredMessageHandlerPermits() {
		return RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
				RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits();
	}

}
