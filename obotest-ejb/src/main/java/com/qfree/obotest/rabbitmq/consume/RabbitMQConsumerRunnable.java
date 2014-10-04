package com.qfree.obotest.rabbitmq.consume;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
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
	private static final long THROTTLE_WAITING_LOOP_SLEEP_MS = 200;

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

	private RabbitMQConsumerHelper messageConsumerHelper = null;

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

		logger.info("Starting RabbitMQ message consumer...");

		try {
			messageConsumerHelper.openConnection();
			try {
				messageConsumerHelper.openChannel();
				try {

					messageConsumerHelper.configureConsumer();
					logger.info("Waiting for image(s).");

					while (true) {

						// Update "throttled_ProducerMsgQueue", if necessary:
						int remainingCapacity = RabbitMQProducerController.producerMsgQueue.remainingCapacity();
						//						logger.info("QRemainingCapacity = {}, throttled_ProducerMsgQueue={}", remainingCapacity,
						//								new Boolean(throttled_ProducerMsgQueue));
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
						int NumUnacknowledgeCDIEvents = RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
								- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits();
						//						logger.info("NumUnacknowledgeCDIEvents = {}, throttled_UnacknowledgedCDIEvents={}",
						//								NumUnacknowledgeCDIEvents,
						//								new Boolean(throttled_UnacknowledgedCDIEvents));
						if (throttled_UnacknowledgedCDIEvents) {
							if (NumUnacknowledgeCDIEvents <= UNACKNOWLEDGED_CDI_EVENTS_LOW_WATER) {
								logger.info("Consumption throttling based on unacknowldeged CDI events is now *off*");
								throttled_UnacknowledgedCDIEvents = false;
							}
						} else {
							if (NumUnacknowledgeCDIEvents >= UNACKNOWLEDGED_CDI_EVENTS_HIGH_WATER) {
								logger.info("Consumption throttling based on unacknowldeged CDI events is now *on*");
								throttled_UnacknowledgedCDIEvents = true;
							}
						}

						throttled = throttled_ProducerMsgQueue || throttled_UnacknowledgedCDIEvents;

						logger.info(
								"Unack={}, Hndlr={}, Q={}, QThrot={}, UnackThrot={}, throt={}",
								RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits(),
								RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits(),
								RabbitMQProducerController.producerMsgQueue.remainingCapacity(),
								new Boolean(RabbitMQConsumerRunnable.throttled_ProducerMsgQueue),
								new Boolean(RabbitMQConsumerRunnable.throttled_UnacknowledgedCDIEvents),
								new Boolean(RabbitMQConsumerRunnable.throttled)
								);

						if (!throttled) {

							try {
								messageConsumerHelper.handleDeliveries();
							} catch (InterruptedException e) {
								/*
								 * Code elsewhere could be requesting that this
								 * thread be terminated. This is checked for below.
								 */
								logger.info("InterruptedException received.");
							} catch (InvalidProtocolBufferException e) {
								logger.info(
										"InvalidProtocolBufferException received. The RabbitMQ connection will close.",
										e);
								break;
							} catch (IOException e) {
								logger.error("IOException received. The RabbitMQ connection will close.", e);
								break;
							} catch (ShutdownSignalException e) {
								logger.info("ShutdownSignalException received. The RabbitMQ connection will close.", e);
								break;
							} catch (ConsumerCancelledException e) {
								logger.info("ConsumerCancelledException received. The RabbitMQ connection will close.",
										e);
								break;
							} catch (Throwable e) {
								// We log the exception, but do not terminate this thread.
								logger.error("Unexpected exception caught.", e);
							}

						} else {
							/*
							 * Wait a short while to allow the condition that triggered 
							 * the throttling to recover, and then run through the loop
							 * again.
							 */
							logger.info("Throttled. Sleeping for {} ms", THROTTLE_WAITING_LOOP_SLEEP_MS);
							try {
								Thread.sleep(THROTTLE_WAITING_LOOP_SLEEP_MS);
							} catch (InterruptedException e) {
							}
						}

						logger.trace("Checking if shutdown was requested...");
						if (RabbitMQConsumerController.state == RabbitMQConsumerControllerStates.STOPPED) {
							logger.info("Request to stop detected. This thread will terminate.");
							break;
						}

						//						logger.info("************** Sleeping for 0.5s to simulate throttling ***************");
						//						try {
						//							Thread.sleep(500);
						//						} catch (InterruptedException e) {
						//						}

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
}
