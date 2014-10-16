package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController.RabbitMQProducerControllerStates;
import com.rabbitmq.client.Channel;

/*
 * This class is instantiated explicitly with "new"; hence, it cannot be managed
 * by the Java EE application container. Therefore, it makes no sense to use
 * the annotations, @Stateless, or @LocalBean here. For the same reason it is 
 * not possible to use dependency injection so the @EJB annotation cannot be 
 * used either.
 */
//@Stateless
//@LocalBean
public class RabbitMQProducerRunnable implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerRunnable.class);

	private RabbitMQProducerHelper messageProducerHelper = null;

	/*
	 * This variable measures how long termination has been deferred because
	 * the conditions necessary for termination have not been met. This is
	 * used to implement a simple safety net to avoid getting caught in an
	 * endless loop when shutting down the producer threads.
	 */
	private long terminationRequestedTime = 0;

	/*
	 * This constructor is necessary, since this is a stateless session bean,
	 * even though all instances of this bean used by the application are 
	 * created with the "new" operator and use a constructor with arguments.
	 */
	public RabbitMQProducerRunnable() {
	}

	public RabbitMQProducerRunnable(RabbitMQProducerHelper messageProducerHelper) {
		super();
		this.messageProducerHelper = messageProducerHelper;
	}

	@Override
	public void run() {

		logger.debug("Starting RabbitMQ message producer...");

		try {
			messageProducerHelper.openConnection();
			try {
				messageProducerHelper.openChannel();

				Channel channel = messageProducerHelper.getChannel();

				if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
					/*
					 * Enable TX mode on this channel.
					 */
					channel.txSelect();
				} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
					/*
					 * Enable publisher acknowledgements (lightweight publisher 
					 * confirms) on this channel.
					 */
					channel.confirmSelect();
				} else {
					// For other cases, there is nothing to do here.
				}

				/*
				 * This map will hold all of the RabbitMQMsgAck objects that will
				 * be used to acknowledge the messages that were originally
				 * consumed in order to create the messages that are published
				 * in this thread. The keys for this objects are the sequence
				 * numbers for the messages published in this thread. These
				 * sequence numbers will all eventually be sent back by the 
				 * RabbitMQ broker that receives these published messages after
				 * that broker determines that the message has either been 
				 * persisted, delivered to another consumer or whatever, i.e.,
				 * the message has been confirmed at the other end. These 
				 * sequence numbers that are sent back by the broker are received
				 * in this thread by either a "ConfirmListener" (corresponding
				 * to "acks" or "nacks") or possibly a "ReturnListener" (for 
				 * failed deliveries when basicPublish is called with "mandatory"
				 * or "immediate" flags set). In this way, the returned sequence
				 * numbers become the deliveryTags for the messages).
				 * 
				 * As the deliveryTags (sequence numbers) are received in this 
				 * thread, the corresponding RabbitMQMsgAck objects are placed in
				 * the acknowledgement queue for the appropriate consumer thread
				 * (after first setting their attributes appropriately for an 
				 * "ack" or a "nack") so that the original message can be "acked"
				 * or "nacked" when the acknowledgement queue is processed by the
				 * consumer thread.
				 * 
				 * Note:
				 * 1. The "mandatory" flag ........................................................
				 * 2. The RabbitMQ server does not support the "immediate" flag
				 */
				SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms = Collections
						.synchronizedSortedMap(new TreeMap<Long, RabbitMQMsgAck>());

				//				while (publishNextMessage()) {
				while (true) {

					try {
						messageProducerHelper.handlePublish();
					} catch (InterruptedException e) {
						logger.warn("InterruptedException received.");
					} catch (IOException e) {
						/*
						* TODO Catch this exception in handlePublish()? At any rate, the original consumed
						*      message should probably be acked/rejected/dead-lettered, either there or here.
						*/
						logger.error("IOException received.", e);
					}

					if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
						// Is there anything to do here?
					}

					//if (RabbitMQProducerController.state == RabbitMQProducerControllerStates.STOPPED) {
					//	if (stoppingNowIsOK(acknowledgementQueue)) {
					//		break;
					//	}
					//} else {
					//	terminationRequestedTime = 0;  // reset, if necessary
					//}

					if (RabbitMQProducerController.state == RabbitMQProducerControllerStates.STOPPED) {
						logger.info("Request to stop detected. This thread will terminate.");
						break;
					}
				}
			} catch (IOException e) {
				logger.error(
						"Exception thrown setting up RabbitMQ channel for conusuming messages. This thread will terminate.",
						e);
			} finally {
				logger.info("Closing RabbitMQ channel...");
				messageProducerHelper.closeChannel();
			}

		} catch (IOException e) {
			//TODO Write out more details about the connection attempt
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
				messageProducerHelper.closeConnection();
			} catch (IOException e) {
				logger.error("Exception caught closing RabbitMQ connection", e);
			}
		}

		logger.info("Thread exiting");

	}

//	/**
//	 * Publishes next message from the producer queue, if there is one.
//	 * @return true if the main loop in the thread's run() method should continue
//	 *         looping and keep publishing messages, or false to stop producing
//	 *         and then terminate
//	 */
//	private boolean publishNextMessage() {
//		boolean continuePublishing = true;
//
//		try {
//			messageProducerHelper.handlePublish();
//		} catch (InterruptedException e) {
//			logger.warn("InterruptedException received.");
//		} catch (ShutdownSignalException e) {
//			// I'm not sure if/when this will occur.
//			logger.info("ShutdownSignalException received. The RabbitMQ connection will close.", e);
//			continuePublishing = false;
//		} catch (ConsumerCancelledException e) {
//			// I'm not sure if/when this will occur.
//			logger.info("ConsumerCancelledException received. The RabbitMQ connection will close.", e);
//			continuePublishing = false;
//		} catch (IOException e) {
//			/*
//			* TODO Catch this exception in handlePublish()? At any rate, the original consumed
//			*      message should probably be acked/rejected/dead-lettered, either there or here.
//			*/
//			logger.error("IOException received.", e);
//		} catch (Throwable e) {
//			// I'm not sure if/when this will occur.
//			// We log the exception, but do not terminate this thread.
//			logger.error("Unexpected exception caught.", e);
//		}
//
//		return continuePublishing;
//	}
}
