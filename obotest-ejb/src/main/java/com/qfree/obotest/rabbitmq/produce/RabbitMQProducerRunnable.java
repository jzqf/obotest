package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
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
	/*
	 * Since this thread is never interrupted via Thread.interrupt(), we don't 
	 * want to block for any length of time so that this thread can respond to 
	 * state changes in a timely fashion. So this timeout should be "small".
	 */
	private static final long RABBITMQ_PRODUCER_TIMEOUT_MS = 1000;
	/*
	 * To signal that these consumer threads should terminate, another thread
	 * will set:
	 * 
	 * RabbitMQProducerController.state == RabbitMQProducerController.STOPPED 
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

	RabbitMQProducerHelper messageProducerHelper = null;

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

		logger.info("Starting RabbitMQ message producer...");

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
		 * 1. The "mandatory" flag is used to tell the broker that the 
		 *    message sent to it *must* be routable to at least one queue.
		 *    If, for any reason, this is not possible, the broker will 
		 *    respond appropriately. If this flag is not set and the 
		 *    broker cannot route the message to a queue, the message  
		 *    will probably be dropped/discarded.
		 * 2. According to https://www.rabbitmq.com/specification.html,
		 *    it appears that the RabbitMQ server might not support the 
		 *    "immediate" flag.
		 */
		SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms = Collections
				.synchronizedSortedMap(new TreeMap<Long, RabbitMQMsgAck>());

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

					/*
					 * This listener is triggered by the broker as it accepts 
					 * messages and does whatever is necessary to persist them
					 * so that they will not be lost if that broker dies.
					 */
					channel.addConfirmListener(new RabbitMQProducerConfirmListener(pendingPublisherConfirms));

					/*
					 * This listener is triggered by the broker for failed 
					 * deliveries if either the "immediate" or "mandatory" flags
					 * are set, but cannot be honoured by the broker.
					 */
					channel.addReturnListener(new RabbitMQProducerReturnListener(pendingPublisherConfirms));

				} else {
					// For other cases, there is nothing to do here.
				}

				long nextPublishSeqNo = 0;  // only used for AckAlgorithms.AFTER_PUBLISHED_CONFIRMED
				while (true) {

					try {
						RabbitMQMsgEnvelope rabbitMQMsgEnvelope;
						rabbitMQMsgEnvelope = RabbitMQProducerController.producerMsgQueue.poll(
								RABBITMQ_PRODUCER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
						if (rabbitMQMsgEnvelope != null) {

							/*
							 * Extract from rabbitMQMsgEnvelope both the outgoing serialized 
							 * protobuf message to be published here as well as the 
							 * RabbitMQMsgAck object that is associated with the original 
							 * consumed RabbitMQ message (containing its delivery tag and other 
							 * details).
							 */
							byte[] messageBytes = rabbitMQMsgEnvelope.getMessage();
							RabbitMQMsgAck rabbitMQMsgAck = rabbitMQMsgEnvelope.getRabbitMQMsgAck();

							if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
								/*
								 * If, for some reason, the message is not actually published 
								 * below in handlePublish(messageBytes), testing shows that
								 * there is no harm calling getNextPublishSeqNo() here again
								 * when trying to send the same or a different message on the
								 * next trip through this loop. In other words, the 
								 * getNextPublishSeqNo() call here seems to fetch the next
								 * value that RabbitMQ manages itself; this call does not force
								 * the sequence number to be incremented. In addition, testing
								 * shows that this sequence number is incremented whether or 
								 * not we call getNextPublishSeqNo() here. So, we do not need
								 * to concern ourself with what to do if an exception is
								 * thrown while publishing a message.
								 */
								nextPublishSeqNo = channel.getNextPublishSeqNo();

								logger.debug("Next msg from producer queue: rabbitMQMsgAck = {}", rabbitMQMsgAck);
							}

							try {
								logger.debug("nextPublishSeqNo = {}. Publishing message...", nextPublishSeqNo);

								messageProducerHelper.handlePublish(messageBytes);

								if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
									/*
									 * This will block here until the RabbitMQ broker that just received
									 * the published message has written the message to disk and performed
									 * some sort of fsync(), which takes a significant time to complete.
									 * Therefore, this acknowledgement algorithm, while very safe, will 
									 * probably be too slow in practice.
									 */
									channel.txCommit();
								}

								if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
										|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
									/*
									 * Enter rabbitMQMsgAck into the acknowledgment queue where it will
									 * be processed by the appropriate consumer thread.
									 */
									rabbitMQMsgAck.queueAck();
								}

								if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
									/*
									 * Enter the RabbitMQMsgAck object into the "pending publisher confirms"
									 * map. This map will be processed by the ConfirmListener added above.
									 */
									logger.debug("Published msg. Entering into pendingPublisherConfirms: "
											+ "nextPublishSeqNo = {}, rabbitMQMsgAck = {}",
											nextPublishSeqNo, rabbitMQMsgAck);
									pendingPublisherConfirms.put(nextPublishSeqNo, rabbitMQMsgAck);
								}

							} catch (IOException e) {
								logger.error("IOException caught publishing a message:", e);
								if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
										|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED
										|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX) {
								/*
									* Even for algorithm AckAlgorithms.AFTER_PUBLISHED_CONFIRMED, this
									* enters the RabbitMQMsgAck object directly into the acknowledgement
									* queue instead of into the "pending publisher confirms" map because
									* we do not expect the broker to send back a publisher confirm. Testing
									* and experience will show if this is the appropriate treatment.
									* TODO Should we dead-letter a message when an IOException is caught?
									*/
								rabbitMQMsgAck.queueNack(true);  // requeue rejected message
								}
							}

						} else {
							/*
							 * This just means that there were no messages in the queue to
							 * publish after waiting the timeout period. This in perfectly 
							 * normal. The timeout is implemented so that this thread can
							 * check whether there has been a request made for it to 
							 * terminate or whatever. 
							 */
						}
					} catch (InterruptedException e1) {
						/*
						 * An interrupt exception was caught while we were waiting
						 * to receive an item from the producer queue. This application
						 * does not currently throw this exception. Regardless, there 
						 * is nothing necessary to do here.						 */
					}

					if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
						logger.debug("{} pending publisher confirms", pendingPublisherConfirms.size());
					}

					if (RabbitMQProducerController.state == RabbitMQProducerControllerStates.STOPPED) {
						if (isOKToStop(pendingPublisherConfirms)) {
							break;
						}
					} else {
						terminationRequestedTime = 0;  // reset, if necessary
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

		logger.debug("pendingPublisherConfirms = {}", pendingPublisherConfirms);
		logger.info("Thread exiting");

	}

	/**
	 * 
	 * @return true if it is OK for the current consumer thread to terminate.
	 */
	private boolean isOKToStop(SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms) {
		boolean stop = false;
		if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {

			if (terminationRequestedTime == 0) {
				// Record when thread termination was initially requested.
				terminationRequestedTime = System.currentTimeMillis();
			}
			// Check that we have not deferred thread termination too long.
			if (System.currentTimeMillis() < terminationRequestedTime + MAX_WAIT_BEFORE_TERMINATION_MS) {

				logger.info("Request to stop detected.");
				/*
				 * Before we allow the producer threads to terminate, we try
				 * to ensure that the "pending confirms" map for this thread is 
				 * empty. Actually, it is not a 
				 * big deal if we let this thread terminate without processinf
				 * all items in this map because the corresponding 
				 * messages will automatically be requeued anyway after the 
				 * connection to the RabbitMQ broker is closed. Of course,this 
				 * will only be OK if the messages are treated in an idempotent 
				 * manner so that requeuing a message will not cause problems. 
				 * But it is possible (though unlikely), that there could still 
				 * be one or more elements in the "pending confirms" map that 
				 * could be "Nacked" by the broker. It may or 
				 * may not be appropriate to allow these messages to be 
				 * automatically requeued. At any rate, it will be most 
				 * beneficial if we can be sure that the "pending confirms" map
				 * is empty, before we let this consumer thread terminate.
				 */
				if (pendingPublisherConfirms.size() == 0) {
					logger.info("This thread will terminate.");
					stop = true;
				} else {
					logger.info(
							"Request to stop detected, but but there are still {} elements in the pending acknowledgement map.",
							pendingPublisherConfirms.size());
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
