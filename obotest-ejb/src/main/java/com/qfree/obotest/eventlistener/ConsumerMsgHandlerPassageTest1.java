package com.qfree.obotest.eventlistener;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

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
	public void processPassage(@Observes @PassageQualifier PassageTest1Event event) {

		/* 
		 * Release the permit that was acquired just before the CDI event that
		 * is received in this methods was fired.
		 */
		logger.debug("Releasing unacknowledged CDI event permit. Unacked permits={}",
				RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits());
		RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.release();

		logger.debug("Unacked permits={}, Handler permits={}, Q={}, throttled={}",
				RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits(),
				RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits(),
				RabbitMQProducerController.producerMsgQueue.remainingCapacity(),
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
		logger.debug("Before acquiring message handler permit. Available permits = {}",
				RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits());
		if (RabbitMQConsumerController.messageHandlerCounterSemaphore.tryAcquire()) {
			logger.debug("After acquiring message handler permit. Available permits = {}",
					RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits());

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
				 * Queue the message for publishing to a RabbitMQ broker. The 
				 * actual publishing will be performed in a RabbitMQ producer 
				 * background thread.
				 */
				logger.debug("Queuing message for delivery [{} bytes]", passageBytes.length);
				/*
				 * Remember that success here only means that the message was queued for
				 * delivery, not that is *was* delivered!
				 * TODO Should we try to follow up that the message *was* delivered successfully?
				 * If so, what should be done in that case?
				 */
				//				boolean success = false;
				boolean success = this.send(passageBytes);
				if (success) {
					logger.debug("Message successfully queued.");
				} else {
					if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {
						rabbitMQMsgAck.setRejected(true);
						rabbitMQMsgAck.setRequeueRejectedMsg(true);
						//TODO We must enter rabbitMQMsgAck into the acknowledgement queue, acknowledgementQueue
						logger.warn("Message could not be queued.");
					} else {
						logger.warn("\n**********\nMessage could not be queued. It will be lost!\n**********");
					}
				}

			} finally {
				RabbitMQConsumerController.messageHandlerCounterSemaphore.release();
				logger.debug("Message handler permit released. Number of free permits = {}",
						RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits());
			}

		} else {
			if (RabbitMQConsumerController.ackAlgorithm == AckAlgorithms.AFTER_SENT) {
				rabbitMQMsgAck.setRejected(true);
				rabbitMQMsgAck.setRequeueRejectedMsg(true);
				//TODO We must enter rabbitMQMsgAck into the acknowledgement queue, acknowledgementQueue
				logger.warn("Permit not acquired to process message.");
			} else {
				logger.warn("\n**********\nPermit not acquired to process message. The message will be lost!\n**********");
			}
		}

	}

	public boolean send(byte[] bytes) {

		logger.debug("Unacked permits={}, Handler permits={}, Q={}, throttled={}",
				RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits(),
				RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits(),
				RabbitMQProducerController.producerMsgQueue.remainingCapacity(),
				new Boolean(RabbitMQConsumerRunnable.throttled)
				);

		logger.debug("RabbitMQProducerController.producerMsgQueue.size() = {}",
				RabbitMQProducerController.producerMsgQueue.size());
		logger.debug("RabbitMQProducerController.producerMsgQueue.remainingCapacity() = {}",
				RabbitMQProducerController.producerMsgQueue.remainingCapacity());

		/*    
		 * If the queue is not full this will enter the message into the queue 
		 * and then return "true". If the queue is full, this call will block 
		 * for up to a certain timeout period. If a queue entry becomes 
		 * available before this timeout period is reached, the message is 
		 * placed in the queue and "true" is returned; otherwise, "false" is 
		 * returned and the sender must deal with the failure.
		 * 
		 * TODO Staying in this loop forever is not good. Implement a better algorithm.
		 */
		logger.debug("Offering a message to the producer blocking queue [{} bytes]...", bytes.length);
		boolean success = false;
		while (!success) {
			try {
				//TODO How long should we block here? We need to think through this behaviour carefully.
				//			success = RabbitMQProducerController.producerMsgQueue.offer(bytes);
				success = RabbitMQProducerController.producerMsgQueue.offer(bytes, PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS,
						TimeUnit.MILLISECONDS);

				logger.debug("Unacked permits={}, Handler permits={}, Q={}, throttled={}, success={}",
						RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits(),
						RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits(),
						RabbitMQProducerController.producerMsgQueue.remainingCapacity(),
						new Boolean(RabbitMQConsumerRunnable.throttled),
						new Boolean(success)
						);

				//TODO "PUBLISHER CONFIRMS" !!!!!!!!!

			} catch (InterruptedException e) {
				//TODO Can we ensure that the message to be queued, i.e., "bytes", is not lost?
				logger.warn("\n********************" +
						"\nInterruptedException caught while waiting to offer a message to the queue. " +
						"\nPerhaps a request has been made to stop the RabbitMQ producer thread(s) in single threaded mode"
						+
						"\nor the RabbitMQ producer thread(s)?" +
						"\nCan we ensure that the message to be queued is not lost?" +
						"\n********************");
			}
			if (success) {
				logger.debug("Message successfully entered into producer blocking queue.");
			} else {
				/*
				 * Either the queue is full, or an interrupt was received while
				 * waiting to publish the message. 
				 */
				//TODO Ensure that a "nack/reject" is sent back to the RabbitMQ broker. Then the message should *not* be lost.
				logger.warn("\n**********\nMessage not entered into producer blocking queue!\n**********");

				logger.info("RabbitMQProducerController.producerMsgQueue.remainingCapacity() = {}",
						RabbitMQProducerController.producerMsgQueue.remainingCapacity());
				logger.info("RabbitMQConsumerRunnable.throttled = {}", RabbitMQConsumerRunnable.throttled);
			}
		}
		return success;
	}

	@PreDestroy
	public void preDestroy() {
		logger.info("This bean is going away...");
	}

}
