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
import com.qfree.obotest.event.PassageTest1Event;
import com.qfree.obotest.protobuf.PassageTest1Protos;
import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelope;
import com.qfree.obotest.rabbitmq.RabbitMQMsgEnvelopeQualifier_async;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.AckAlgorithms;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerRunnable;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;

@Stateless
@LocalBean
public class ConsumerMsgHandlerPassageTest1 implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerMsgHandlerPassageTest1.class);

	private static final long serialVersionUID = 1L;
	private static final long PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS = 1000;

	public ConsumerMsgHandlerPassageTest1() {
		logger.debug("{} instance created", ConsumerMsgHandlerPassageTest1.class.getSimpleName());
	}

	@Asynchronous
	public void processMessage_async(@Observes @RabbitMQMsgEnvelopeQualifier_async PassageTest1Event event) {

		/* 
		 * Release the permit that was acquired just before the CDI event that
		 * is received in this methods was fired.
		 */
		RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();

		/*
		 * UE:  number of Unacknowledged CDI Events
		 * MH:  number of message handlers running
		 * PQ:  number of elements in the producer message queue
		 * AQ:  number of elements in the acknowledgement queue
		 */
		logger.debug(
				"UE={}, MH={}, PQ={}, PQ-Throt={}, UE-Throt={}, Throt={}",
				RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
						- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore
								.availablePermits(),
				RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
						RabbitMQConsumerController.messageHandlerCounterSemaphore
								.availablePermits(),
				RabbitMQProducerController.producerMsgQueue.size(),
				new Boolean(RabbitMQConsumerRunnable.throttled_ProducerMsgQueue),
				new Boolean(RabbitMQConsumerRunnable.throttled_UnacknowledgedCDIEvents),
				new Boolean(RabbitMQConsumerRunnable.throttled)
				);

		RabbitMQMsgAck rabbitMQMsgAck = event.getRabbitMQMsgAck();

		/*
		 * Attempt to acquire a permit to process this message. This mechanism
		 * enables us to know exactly how many message handler threads are
		 * processing incoming messages at any time. This enables us to ensure 
		 * during shutdown that all messages have been processed in the before 
		 * waiting for the producer queue to empty.
		 */
		if (RabbitMQConsumerController.messageHandlerCounterSemaphore.tryAcquire()) {

			try {
				logger.debug("Start processing passage: {}...", event.toString());
				//				try {
				//					logger.debug("Sleeping for 500 ms to simulate doing some work...");
				//					Thread.sleep(500);		// simulate doing some work
				//				} catch (InterruptedException e) {
				//				}
				logger.debug("Finished processing passage: {}...", event.toString());

				/*
				 * Send the result of the processing as a RabbitMQ message to a 
				 * RabbitMQ broker.
				 * 
				 * First, create a RabbitMQ message payload.
				 */
				PassageTest1Protos.PassageTest1.Builder passage = PassageTest1Protos.PassageTest1.newBuilder();
				passage.setImageName(event.getImageName());
				passage.setImage(ByteString.copyFrom(event.getImageBytes()));
				byte[] passageBytes = passage.build().toByteArray();

				/*
				 * Package both the RabbitMQMsgAck that is associated with the
				 * original consumed RabbitMQ message (containing its delivery
				 * tag and other details) and the outgoing serialized protobuf
				 * message in a single object that can be placed in the producer
				 * message blocking queue.
				 */
				//TODO The RabbitMQMsgEnvelope class is quite general. Consider reusing it elsewhere.
				RabbitMQMsgEnvelope rabbitMQMsgEnvelope = new RabbitMQMsgEnvelope(rabbitMQMsgAck, passageBytes);

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
					} else {
						logger.warn("\n**********\nMessage could not be entered into the producer queue."
								+ " It will be lost!\n**********");
					}
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
			} else {
				logger.warn("\n**********\nPermit not acquired to process message."
						+ " The message will be lost!\n**********");
			}
		}

	}

	@Asynchronous
	public void processPassage(@Observes @PassageQualifier PassageTest1Event event) {

		/* 
		 * Release the permit that was acquired just before the CDI event that
		 * is received in this methods was fired.
		 */
		RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();

		/*
		 * UE:  number of Unacknowledged CDI Events
		 * MH:  number of message handlers running
		 * PQ:  number of elements in the producer message queue
		 * AQ:  number of elements in the acknowledgement queue
		 */
		logger.debug(
				"UE={}, MH={}, PQ={}, PQ-Throt={}, UE-Throt={}, Throt={}",
				RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
						- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore
								.availablePermits(),
				RabbitMQConsumerController.MAX_MESSAGE_HANDLERS -
						RabbitMQConsumerController.messageHandlerCounterSemaphore
								.availablePermits(),
				RabbitMQProducerController.producerMsgQueue.size(),
				new Boolean(RabbitMQConsumerRunnable.throttled_ProducerMsgQueue),
				new Boolean(RabbitMQConsumerRunnable.throttled_UnacknowledgedCDIEvents),
				new Boolean(RabbitMQConsumerRunnable.throttled)
				);

		RabbitMQMsgAck rabbitMQMsgAck = event.getRabbitMQMsgAck();

		/*
		 * Attempt to acquire a permit to process this message. This mechanism
		 * enables us to know exactly home many message handler threads are
		 * processing incoming messages at any time. This enables us to ensure 
		 * during shutdown that all messages have been processed in the before 
		 * waiting for the producer queue to empty.
		 */
		if (RabbitMQConsumerController.messageHandlerCounterSemaphore.tryAcquire()) {

			try {
				logger.debug("Start processing passage: {}...", event.toString());
				//				try {
				//					logger.debug("Sleeping for 500 ms to simulate doing some work...");
				//					Thread.sleep(500);		// simulate doing some work
				//				} catch (InterruptedException e) {
				//				}
				logger.debug("Finished processing passage: {}...", event.toString());

				/*
				 * Send the result of the processing as a RabbitMQ message to a 
				 * RabbitMQ broker.
				 * 
				 * First, create a RabbitMQ message payload.
				 */
				PassageTest1Protos.PassageTest1.Builder passage = PassageTest1Protos.PassageTest1.newBuilder();
				passage.setImageName(event.getImageName());
				passage.setImage(ByteString.copyFrom(event.getImageBytes()));
				byte[] passageBytes = passage.build().toByteArray();

				/*
				 * Package both the RabbitMQMsgAck that is associated with the
				 * original consumed RabbitMQ message (containing its delivery
				 * tag and other details) and the outgoing serialized protobuf
				 * message in a single object that can be placed in the producer
				 * message blocking queue.
				 */
				//TODO The RabbitMQMsgEnvelope class is quite general. Consider reusing it elsewhere.
				RabbitMQMsgEnvelope rabbitMQMsgEnvelope = new RabbitMQMsgEnvelope(rabbitMQMsgAck, passageBytes);

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
					} else {
						logger.warn("\n**********\nMessage could not be entered into the producer queue."
								+ " It will be lost!\n**********");
					}
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
			} else {
				logger.warn("\n**********\nPermit not acquired to process message."
						+ " The message will be lost!\n**********");
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
