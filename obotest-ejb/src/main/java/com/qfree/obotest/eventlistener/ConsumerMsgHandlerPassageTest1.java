package com.qfree.obotest.eventlistener;

import java.io.Serializable;

import javax.annotation.PreDestroy;
import javax.ejb.Asynchronous;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.enterprise.event.Observes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.qfree.obotest.protobuf.PassageTest1Protos;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelopeQualifier_async;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelopeQualifier_sync;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;

@Stateless
@LocalBean
/*
 * TODO Implement an interface here to enable flexible dependency injection
 *      For example ConsumerMsgHandler. Then, replace ConsumerMsgHandlerPassageTest1
 *      with ConsumerMsgHandler in RabbitMQConsumerHelperPassageTest1 and use
 *      CDI "Alternatives to select an implementation if there ever is more 
 *      than one.
 */
public class ConsumerMsgHandlerPassageTest1 implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerMsgHandlerPassageTest1.class);

	private static final long serialVersionUID = 1L;

	public ConsumerMsgHandlerPassageTest1() {
		logger.debug("{} instance created", ConsumerMsgHandlerPassageTest1.class.getSimpleName());
	}

	@Asynchronous
	public void processMessage_async(
			@Observes @RabbitMQMsgEnvelopeQualifier_async RabbitMQMsgEnvelope rabbitMQMsgEnvelope) {
		/*
		 * Delegate to the synchronous method.
		 */
		processMessage_sync(rabbitMQMsgEnvelope);
	}

	public void processMessage_sync(
			@Observes @RabbitMQMsgEnvelopeQualifier_sync RabbitMQMsgEnvelope rabbitMQMsgEnvelope) {

		/* 
		 * Release the permit that was acquired just before the CDI event that
		 * is received in this methods was fired.
		 */
		RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();

		//	logger.info("this = {}", this);

		/*
		 * UE:  number of Unacknowledged CDI Events
		 * MH:  number of message handlers running
		 * PQ:  number of elements in the producer message queue
		 */
		logger.debug("UE={}, MH={}, PQ={}",
				RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
						- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore
								.availablePermits(),
				RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
						RabbitMQConsumerController.messageHandlerCounterSemaphore
								.availablePermits(),
				RabbitMQProducerController.producerMsgQueue.size()
				);

		/*
		 * Extract the objects packaged in the RabbitMQMsgEnvelope parameter.
		 */
		RabbitMQMsgAck rabbitMQMsgAck = rabbitMQMsgEnvelope.getRabbitMQMsgAck();
		byte[] protobufMsgInBytes = rabbitMQMsgEnvelope.getMessage();

		/*
		 * Attempt to acquire a permit to process this message. This mechanism
		 * enables us to know exactly how many message handler threads are
		 * processing incoming messages at any time. This enables us to ensure 
		 * during shutdown that all messages have been processed in the before 
		 * waiting for the producer queue to empty.
		 */
		if (RabbitMQConsumerController.messageHandlerCounterSemaphore.tryAcquire()) {

			try {
				logger.debug("Start processing message data: Incoming message = {}", rabbitMQMsgEnvelope);

				/*
				 * De-serialize the Protobuf message bytes into Java objects.
				 */
				PassageTest1Protos.PassageTest1 passage = PassageTest1Protos.PassageTest1
						.parseFrom(protobufMsgInBytes);
				String filename = passage.getImageName();
				byte[] imageBytes = passage.getImage().toByteArray();

				//				try {
				//					logger.debug("Sleeping for 100 ms to simulate doing some work...");
				//					Thread.sleep(100);		// simulate doing some work
				//				} catch (InterruptedException e) {
				//				}

				/*
				 * Send the result of the processing as a RabbitMQ message to a 
				 * RabbitMQ broker.
				 * 
				 * First, create a RabbitMQ message payload.
				 */
				PassageTest1Protos.PassageTest1.Builder passageBuilder = PassageTest1Protos.PassageTest1.newBuilder();
				passageBuilder.setImageName(filename);
				passageBuilder.setImage(ByteString.copyFrom(imageBytes));
				byte[] protobufMsgOutBytes = passageBuilder.build().toByteArray();

				/*
				 * Insert the outgoing serialized Protobuf message in the same
				 * RabbitMQMsgEnvelope object that we received above. This
				 * RabbitMQMsgEnvelope object still contains the RabbitMQMsgAck 
				 * object that is associated with the originally consumed 
				 * RabbitMQ message (containing its delivery tag and other 
				 * details). This RabbitMQMsgAck will eventually be used to 
				 * acknowledge the originally consumed message.
				 */
				rabbitMQMsgEnvelope.setMessage(protobufMsgOutBytes);

				logger.debug("Finished processing message data. Outgoing message = {}", rabbitMQMsgEnvelope);

				/*
				 * Queue the message for publishing to a RabbitMQ broker. The 
				 * actual publishing will be performed in a RabbitMQ producer 
				 * background thread.
				 * 
				 * Remember that success here only means that the message was 
				 * queued for delivery, not that is *was* delivered!
				 */
				boolean success = this.send(rabbitMQMsgEnvelope);
				if (success) {
					logger.debug("Message successfully entered into the producer queue.");
				} else {
					if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
							|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX
							|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
						rabbitMQMsgAck.queueNack(true);	// requeue Nacked message
						logger.warn("Message could not be entered into the producer queue."
								+ " The message will be requeued on the broker.");
					} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_RECEIVED) {
						logger.error("Message could not be entered into the producer queue. It will be lost!");
					} else {
						// Obviously, the message will be lost.
						logger.error("Unhandled case: RabbitMQConsumerController.ackAlgorithm = {}",
								RabbitMQConsumerController.ackAlgorithm);
					}
				}

			} catch (InvalidProtocolBufferException e) {

				/*
				* Dead-letter the message, but not if ackmode=AFTER_RECEIVED
				* since the Ack will already have been sent.
				*/
				if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
						|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX
						|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
					rabbitMQMsgAck.queueNack(false);	// discard/dead-letter the message
					logger.error(
							"An InvalidProtocolBufferException was thrown. The message has been discarded/dead-lettered."
									+ " Exception details:", e);
				} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_RECEIVED) {
					logger.error(
							"An InvalidProtocolBufferException was thrown. The message will be lost!"
									+ " Exception details:", e);
				} else {
					// Obviously, the message will be lost.
					logger.error("Unhandled case: RabbitMQConsumerController.ackAlgorithm = {}",
							RabbitMQConsumerController.ackAlgorithm);
				}

			} finally {
				RabbitMQConsumerController.messageHandlerCounterSemaphore.release();
				logger.debug("Message handler permit released. Number of free permits = {}",
						RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits());
			}

		} else {
			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED
					|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_TX
					|| RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_PUBLISHED_CONFIRMED) {
				rabbitMQMsgAck.queueNack(true);	// requeue Nacked message
				logger.warn("Permit not acquired to process message. The message will be requeued on the broker.");
			} else if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_RECEIVED) {
				logger.error("Permit not acquired to process message.  The message will be lost!");
			} else {
				// Obviously, the message will be lost.
				logger.error("Unhandled case: RabbitMQConsumerController.ackAlgorithm = {}",
						RabbitMQConsumerController.ackAlgorithm);
			}
		}

	}

	public boolean send(RabbitMQMsgEnvelope rabbitMQMsgEnvelope) {
		/*
		 * Log a warning if the producer queue is over 90% full. Hopefully, this
		 * will never occur if we choose a value large enough. There are two
		 * mechanisms implemented to limit the number of items offered to this
		 * queue, depending on which acknowledgement mode is being used. Both
		 * mechanisms are always enabled, but because of the different behaviour 
		 * for the two acknowledgement modes, one mechanism becomes active for
		 * one mode and the other mechanism becomes active for the other mode:
		 * 
		 * 1. ackAlgorithm = AFTER_RECEIVED:  The size of the producer queue is
		 *    limited by consumer throttling based on producer queue size. This 
		 *    is performed in RabbitMQConsumerRunnable. If this queue exceeds a
		 *    certain limit, the consumer threads will stop consuming messages.
		 *    While this is effective, there is a feedback lag, so the upper
		 *    limit imposed in RabbitMQConsumerRunnable must e conservative so
		 *    that the producer queue does not exceed its maximum length AFTER
		 *    the consumer threads are throttled (since there may still be
		 *    unacknowledged CDIevents and message handlers that are still 
		 *    running that will add additional entries into the producer queue.
		 *
		 * 2. ackAlgorithm = AFTER_SENT:  The size of the producer queue is
		 *    limited by the "prefetch count" specified by 
		 *    Channel.basicQos(...). In effect, this also performs a type of 
		 *    throttling on the incoming messages, but the mechanism is 
		 *    completely different. The "prefetch count" does not have the 
		 *    same effect for ackAlgorithm = AFTER_RECEIVED because in that mode
		 *    the acknowledgement is sent immediately after receiving the
		 *    message; hence, the number of unacknowledged messages cannot grow
		 *    to a large enough value to effectively throttle the rate that
		 *    incoming messages are consumed.
		 */
		if (RabbitMQProducerController.producerMsgQueue.size() > 0.9 * RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH) {
			logger.warn("Acknowledgement queue is over 90% full. Current size = {}."
					+ " It may be necessary to increase the maximum capacity"
					+ ", but this can also happen if the producer threads are stopped.",
					RabbitMQProducerController.producerMsgQueue.size());
		}

		/*    
		 * Offer the message to the producer queue. Since the producer queue 
		 * should, ideally, never fill up (see the comments above where a
		 * warning is logged if the queue starts to fill up for an explanation
		 * of why this is so), we do not go to heroic lengths to treat the case
		 * where the offer block here.
		 */
		logger.debug("Offering a message to the producer queue [{} bytes]...", rabbitMQMsgEnvelope.getMessage().length);
		return RabbitMQProducerController.producerMsgQueue.offer(rabbitMQMsgEnvelope);
	}

	@PreDestroy
	public void preDestroy() {
		logger.info("This bean is going away...");
	}

}
