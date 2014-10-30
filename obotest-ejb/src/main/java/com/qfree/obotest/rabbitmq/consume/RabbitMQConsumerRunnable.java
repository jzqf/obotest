package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.enterprise.event.ObserverException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerControllerStates;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

/*
 * This class is instantiated explicitly with "new"; hence, it cannot be managed
 * by the Java EE application container. Therefore, it makes no sense to use
 * the annotations, @Stateless, or @LocalBean here. For the same reason it is 
 * not possible to use dependency injection so the @EJB & @Inject annotations 
 * cannot be used either.
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

	/*
	 * To signal that these consumer threads should terminate, another thread
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
	 * 
	 * At the time this was written, all conditions evaluate the same for each 
	 * consumer thread; hence, this variable as well as the other "throttled" 
	 * variables, could be made "public static". However, in order to be able to
	 * handle conditions that might be different for different consumer thread, 
	 * these variables have been made private member attributes with getters.
	 */
	private volatile boolean throttled = false;
	//	public static volatile boolean throttled = false;
	/**
	 * Set to true when the number of free elements available in the 
	 * producerMsgQueue queue drops below QUEUE_REMAINING_CAPACITY_LOW_WATER. It
	 * will then stay false until the number of free elements subsequently rises
	 * above QUEUE_REMAINING_CAPACITY_HIGH_WATER.
	 */
	private volatile boolean throttled_ProducerMsgQueue = false;
	//	public static volatile boolean throttled_ProducerMsgQueue = false;
	/**
	 * Set to true when the number of free elements available in the 
	 * producerMsgQueue queue drops below QUEUE_REMAINING_CAPACITY_LOW_WATER. It
	 * will then stay false until the number of free elements subsequently rises
	 * above QUEUE_REMAINING_CAPACITY_HIGH_WATER.
	 */
	private volatile boolean throttled_UnacknowledgedCDIEvents = false;
	//	public static volatile boolean throttled_UnacknowledgedCDIEvents = false;

	/*
	 * Set in the constructor
	 */
	private RabbitMQConsumerHelper messageConsumerHelper = null;

	private Channel channel = null;

	/*
	 * This queue holds the RabbitMQ delivery tags and other details for 
	 * messages that are processed in other threads but which must be 
	 * acked/nacked in this consumer thread. A new queue is created each
	 * time a channel is opened because the delivery tags that are used
	 * to ack/nack the consumed messages are specific to the channel 
	 * used to consume the original messages.
	 */
	private final BlockingQueue<RabbitMQMsgAck> acknowledgementQueue = new LinkedBlockingQueue<>(
			RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH);

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

	public boolean isThrottled() {
		return throttled;
	}

	public boolean isThrottled_ProducerMsgQueue() {
		return throttled_ProducerMsgQueue;
	}

	public boolean isThrottled_UnacknowledgedCDIEvents() {
		return throttled_UnacknowledgedCDIEvents;
	}

	public BlockingQueue<RabbitMQMsgAck> getAcknowledgementQueue() {
		return acknowledgementQueue;
	}

	@Override
	public void run() {

		logger.info("Starting RabbitMQ message consumer");

		try {
			messageConsumerHelper.openConnection();
			try {
				messageConsumerHelper.openChannel();
				try {

					channel = messageConsumerHelper.getChannel();

					//					/*
					//					 * This queue holds the RabbitMQ delivery tags and other details for 
					//					 * messages that are processed in other threads but which must be 
					//					 * acked/nacked in this consumer thread. A new queue is created each
					//					 * time a channel is opened because the delivery tags that are used
					//					 * to ack/nack the consumed messages are specific to the channel 
					//					 * used to consume the original messages.
					//					 */
					//					BlockingQueue<RabbitMQMsgAck> acknowledgementQueue = new LinkedBlockingQueue<>(
					//							RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH);

					//					messageConsumerHelper.setAcknowledgementQueue(acknowledgementQueue);

					messageConsumerHelper.configureConsumer();
					logger.info("Waiting for messages...");

					//					/*
					//					 * These are for testing only. Delete after things work OK.
					//					 * These counters actually count the number of trips through
					//					 * the "while" loop and the number of times that 
					//					 * handleNextDelivery() is called. But occasionally a
					//					 * message will not be consumed (if there are too many
					//					 * unconfirmed messages or whatever), so these do not 
					//					 * actually count the number of messages consumed and
					//					 * produced!
					//					 */
					//					final long NUM_MSGS_TO_CONSUME = 1000;
					//					long msgs_consumed = 0;

					while (true) {

						if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.RUNNING) {

							// Update "throttled_ProducerMsgQueue", if necessary:
							int remainingCapacity = RabbitMQProducerController.producerMsgQueue.remainingCapacity();
							if (throttled_ProducerMsgQueue) {
								if (remainingCapacity >= QUEUE_REMAINING_CAPACITY_HIGH_WATER) {
									logger.debug("Consumption throttling based on producer queue size is now *off*");
									throttled_ProducerMsgQueue = false;
								}
							} else {
								if (remainingCapacity <= QUEUE_REMAINING_CAPACITY_LOW_WATER) {
									logger.debug("Consumption throttling based on producer queue size is now *on*");
									throttled_ProducerMsgQueue = true;
								}
							}

							// Update "throttled_UnacknowledgedCDIEvents", if necessary:
							int numUnacknowledgeCDIEvents = RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
									- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore
											.availablePermits();
							if (throttled_UnacknowledgedCDIEvents) {
								if (numUnacknowledgeCDIEvents <= UNACKNOWLEDGED_CDI_EVENTS_LOW_WATER) {
									logger.debug("Consumption throttling based on unacknowldeged CDI events is now *off*");
									throttled_UnacknowledgedCDIEvents = false;
								}
							} else {
								if (numUnacknowledgeCDIEvents >= UNACKNOWLEDGED_CDI_EVENTS_HIGH_WATER) {
									logger.debug("Consumption throttling based on unacknowldeged CDI events is now *on*");
									throttled_UnacknowledgedCDIEvents = true;
								}
							}

							throttled = throttled_ProducerMsgQueue || throttled_UnacknowledgedCDIEvents;

							/*
							 * UE:  number of Unacknowledged CDI Events
							 * MH:  number of message handlers running
							 * PQ:  number of elements in the producer message queue
							 * AQ:  number of elements in the acknowledgement queue
							 */
							logger.info(
									"UE={}, MH={}, PQ={}, AQ={}, PQ-Throt={}, UE-Throt={}, Throt={}",
									numUnacknowledgeCDIEvents,
									RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
											RabbitMQConsumerController.messageHandlerCounterSemaphore
													.availablePermits(),
									RabbitMQProducerController.producerMsgQueue.size(),
									acknowledgementQueue.size(),
									new Boolean(throttled_ProducerMsgQueue),
									new Boolean(throttled_UnacknowledgedCDIEvents),
									new Boolean(throttled)
									);

							if (!throttled) {

								//								if (msgs_consumed < NUM_MSGS_TO_CONSUME) {
								//									msgs_consumed += 1;
								//									//logger.info("\n\nAbout to consume message...\n");

								if (RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.tryAcquire()) {

									/*
									 * The "deliveryTag" attribute of rabbitMQMsgAck needs to be
									 * set in handleNextDelivery().
									 */
									RabbitMQMsgAck rabbitMQMsgAck = new RabbitMQMsgAck(acknowledgementQueue);

									/*
									 * The "message"attribute of rabbitMQMsgEnvelope needs to be
									 * set in handleNextDelivery().
									 */
									RabbitMQMsgEnvelope rabbitMQMsgEnvelope = new RabbitMQMsgEnvelope(rabbitMQMsgAck,
											null);

									try {

										messageConsumerHelper.handleNextDelivery(rabbitMQMsgEnvelope);

										/*
										 * rabbitMQMsgAck.hasMessage() is true if a message was consumed 
										 * in handleNextDelivery() and its delivery tag stored in 
										 * rabbitMQMsgAck. It is possible that under normal oeration a 
										 * message is *not* consumed in handleNextDelivery(). This will
										 * happen if, e.g., the method waiting for a message times out.
										 * If that happens, we will simply try again on the next trip
										 * through this loop.
										 */
										if (rabbitMQMsgAck.hasMessage()) {
											if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_RECEIVED) {
												rabbitMQMsgAck.setRejected(false);  // message should be Acked, i.e., not Nacked
												acknowledgeMsg(rabbitMQMsgAck);
											}
										} else {
											/*
											 * If a message was *not* consumed in handleNextDelivery(...), then
											 * neither will handleNextDelivery(...) have fired a CDI event or
											 * called a method to process the message. This means that the target
											 * method for processing the message will not run and, therefore, the 
											 * "unacknowledgeCDIEventsCounterSemaphore" semaphore permit will not
											 * be released. If that is the case, we release the permit here. 
											 * If this is *not* done, the number of permits acquired will keep
											 * rising until consumer throttling is engaged, at which point the
											 * system will lock in that mode because there will be no code
											 * running to release these acquired permits.
											 */
											RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();
										}

									} catch (InterruptedException e) {

										/*
										* If this exception is thrown, it is unlikely that a message was
										* received in handleNextDelivery(); therefore, there is no need 
										* to nack/reject any message.
										*/
										logger.warn("InterruptedException received.", e);
										RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();

									} catch (ShutdownSignalException e) {

										/*
										 * I'm not sure under which conditions this exception might be thrown.
										 * 
										 * If this exception is thrown, no message will have been received
										 * in handleNextDelivery(); therefore, there is no need to nack/reject
										 * any message.
										 */
										logger.info(
												"ShutdownSignalException received. The RabbitMQ connection will close.",
												e);
										RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();
										break;

									} catch (ConsumerCancelledException e) {

										/*
										 * I'm not sure under which conditions this exception might be thrown.
										 * 
										 * If this exception is thrown, no message will have been received
										 * in handleNextDelivery(); therefore, there is no need to nack/reject
										 * any message.
										 */
										logger.info(
												"ConsumerCancelledException received. The RabbitMQ connection will close.",
												e);
										RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();
										break;

									} catch (ObserverException | IllegalArgumentException e) {

										logger.error("An was exception caught, probably from " +
												"messageConsumerHelper.handleNextDelivery(rabbitMQMsgEnvelope):", e);
										/*
										 * These exceptions can be thrown by Event.fire(...) in 
										 * handleNextDelivery(...). The CDI event will not be received
										 * by any code, so we need to release the permit here that was
										 * acquired above.
										 */
										RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();
										/*
										 * The message consumed in handleNextDelivery(...) must be Nacked.
										 * TODO Should we introduce a configuration parameter for requeuing or dead-lettering the message?
										 */
										rabbitMQMsgAck.setRejected(true);
										rabbitMQMsgAck.setRequeueRejectedMsg(false);  // discard/dead-letter the message
										acknowledgeMsg(rabbitMQMsgAck);

										// } catch (com.qfree.obotest.rabbitmq.RabbitMQMessageNotHandledException e) {
										//
										// 	/*
										// 	 * This custom exception can be implemented if we want to
										// 	 * be able to signal from handleNextDelivery(...) that 
										// 	 * something went wrong and the message must be Nacked. We
										// 	 * also release the semaphore permit that otherwise would not 
										// 	 * be released.
										// 	 */
										// 	RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();
										// 	rabbitMQMsgAck.setRejected(true);
										// 	rabbitMQMsgAck.setRequeueRejectedMsg(false);  // discard/dead-letter the message
										// 	acknowledgeMsg(rabbitMQMsgAck);

									} catch (Throwable e) {

										// I'm not sure if/when this will ever occur.
										// We log the exception, but do not terminate this thread.
										logger.error("Unexpected exception caught:", e);
										rabbitMQMsgAck.setRejected(true);
										rabbitMQMsgAck.setRequeueRejectedMsg(false);  // discard/dead-letter the message
										acknowledgeMsg(rabbitMQMsgAck);

									}

								} else {
									logger.warn("Permit not acquired to consume message.");
								}

								//								} else {
								//									try {
								//										logger.info("Sleeping 2s...");
								//										Thread.sleep(2 * LONG_SLEEP_MS);
								//									} catch (InterruptedException e) {
								//									}
								//								}  // if(msgs_consumed<NUM_MSGS_TO_CONSUME)

							}
						}

						logger.debug("RabbitMQConsumerController.state = {}", RabbitMQConsumerController.state);

						if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
								|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX
								|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
							acknowledgeMsgsInQueue(acknowledgementQueue);
						}

						/*
						 * Sleep for a short period, as appropriate. In normal operation
						 * while we are consuming messages, we will not pause here at all.
						 */
						sleepALittleBit(throttled, acknowledgementQueue);

						if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.STOPPED) {
							if (isOKToStop(acknowledgementQueue)) {
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

	/**
	 * Processes the elements in the specified acknowledgement queue, if the
	 * queue has any elements.
	 * @param acknowledgementQueue
	 */
	private void acknowledgeMsgsInQueue(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue) {
		if (acknowledgementQueue.size() > 0) {
			logger.debug("Processing {} elements from the acknowledgement queue...", acknowledgementQueue.size());
			RabbitMQMsgAck rabbitMQMsgAck = acknowledgementQueue.poll();
			while (rabbitMQMsgAck != null) {
				logger.debug("Delivery tag = {}", rabbitMQMsgAck.getDeliveryTag());
				acknowledgeMsg(rabbitMQMsgAck);
				rabbitMQMsgAck = acknowledgementQueue.poll();
			}
		}
	}

	/**
	 * Acknowledges a consumed RabbitMQ message, according to the supplied
	 * RabbitMQMsgAck argument.
	 * @throws IOException 
	 */
	public void acknowledgeMsg(RabbitMQMsgAck rabbitMQMsgAck) {
		try {
			if (!rabbitMQMsgAck.isRejected()) {
				// Acknowledge the message, and only this message.
				logger.debug("Acking delivery tag = {}", rabbitMQMsgAck.getDeliveryTag());
				//logger.debug("Acking message: {}", rabbitMQMsgAck);
				channel.basicAck(rabbitMQMsgAck.getDeliveryTag(), false);
			} else {
				/*
				 * Reject the message, and request that it be requeued or not 
				 * according to the value of rabbitMQMsgAck.isRequeueRejectedMsg().
				 */
				logger.warn("Nacking delivery tag = {}", rabbitMQMsgAck.getDeliveryTag());
				//logger.warn("Nacking message: {}", rabbitMQMsgAck);
				channel.basicNack(rabbitMQMsgAck.getDeliveryTag(), false, rabbitMQMsgAck.isRequeueRejectedMsg());
			}
		} catch (IOException e) {
			// This is very unlikely, but:
			//TODO What should I do with the rabbitMQMsgAck message here?
			logger.error("Exception thrown acknowledging a RabbitMQ message. rabbitMQMsgAck = {}, exception = {}",
					rabbitMQMsgAck, e);
		}
	}

	private void sleepALittleBit(boolean throttled, BlockingQueue<RabbitMQMsgAck> acknowledgementQueue) {

		long sleepMs = SHORT_SLEEP_MS;
		if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.RUNNING) {
			if (!throttled) {
				/*
				 * This is the normal case where we are:
				 *   1. consuming messages (not DISABLED) and 
				 *   2. message consumption is not throtted.
				 */
				sleepMs = 0;
			}
		} else {
			/*
			 * This covers both states: DISABLED & STOPPED
			 */
			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
					|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX
					|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {

				if (acknowledgementQueue.size() > 0) {
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
		}

		if (sleepMs > 0) {
			logger.debug("Sleeping for {} ms", sleepMs);
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
	private boolean isOKToStop(BlockingQueue<RabbitMQMsgAck> acknowledgementQueue) {
		boolean stop = false;
		if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
				|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX
				|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {

			if (terminationRequestedTime == 0) {
				// Record when thread termination was initially requested.
				terminationRequestedTime = System.currentTimeMillis();
			}
			// Check that we have not deferred thread termination too long.
			if (System.currentTimeMillis() < terminationRequestedTime + MAX_WAIT_BEFORE_TERMINATION_MS) {

				logger.info("Request to stop detected.");
				/*
				 * Before we allow the consumer threads to terminate, we try
				 * to ensure that the acknowledgement queue for this thread is 
				 * empty and that it will *stay* empty. Actually, it is not a 
				 * big deal if we let this thread terminate without performing a
				 * positive acknowledgement for a few messages because those 
				 * messages will automatically be requeued anyway after the 
				 * connection to the RabbitMQ broker is closed. Of course,this 
				 * will only be OK if the messages are treated in an idempotent 
				 * manner so that requeuing a message will not cause problems. 
				 * But it is possible (though unlikely), that there could still 
				 * be one or more elements in the acknowledgement queue that are
				 * are for nacking or rejecting (dead-lettering) messages. It 
				 * may not be appropriate to allow these messages to be 
				 * automatically requeued. At any rate, it will be most 
				 * beneficial if we can be sure that the acknowledgement queue 
				 * is empty, and will stay that way, before we let this consumer
				 * thread terminate.
				 * 
				 * But to be more-or-less sure that an empty acknowledgement
				 * queue stays that way, we check *first* that:
				 * 
				 *   1. There are no unacknowledged CDI events.
				 *   2.	There are no message handlers still running
				 *   3. The producer message queue is empty.
				 * 
				 * These checks are similar to those made in
				 * RabbitMQProducerController.shutdown(), but here I do not
				 * wait if the condition is not true and neither to I try to
				 * force the producer threads to run.
				 */
				if (unacknowledgedCDIEventPermits() == 0) {
					if (acquiredMessageHandlerPermits() == 0) {
						if (RabbitMQProducerController.producerMsgQueue.size() == 0) {
							/*
							 * If the consumer threads are not first disabled, it may
							 * be difficult to stop the consumer threads because the 
							 * acknowledgment queue may never become empty. But this 
							 * should not be looked upon as a problem. All that is 
							 * necessary is to first disable the consumer threads, AND
							 * THEN stop them a short while later after the consumed 
							 * messages have been processed.
							 */
							if (acknowledgementQueue.size() == 0) {
								logger.info("This thread will terminate.");
								stop = true;
							} else {
								logger.info(
										"Request to stop detected, but but there are still {} elements in the acknowledgement queue.",
										acknowledgementQueue.size());
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
			logger.info("Request to stop detected. This thread will be allowed terminate.");
			stop = true;
		}
		return stop;
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
