package com.qfree.obotest.eventlistener;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.ejb.Asynchronous;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.qfree.obotest.event.PassageTest1Event;
import com.qfree.obotest.protobuf.PassageTest1Protos;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;

@Stateless
@LocalBean
public class PassageTest1Handler implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final long PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS = 10000;

	private static final Logger logger = LoggerFactory.getLogger(PassageTest1Handler.class);

	@Inject
	private RabbitMQConsumerController rabbitMQConsumerController;

	@Inject
	private RabbitMQProducerController rabbitMQProducerController;

	public PassageTest1Handler() {
		logger.info("PassageTest1Handler instance created");
	}

	@Asynchronous
	public void processPassage(@Observes @PassageQualifier PassageTest1Event event) {

		/*
		 * TODO Attempt to increase semaphore count by one (acquire a permit).
		 * If successful:
		 * 		* start "try" block. 
		 * 		* Log the number of permits in use.
		 * 		* Release semaphore in "finally" block
		 * If semaphore permit *not* acquired, (hopefully, this will never happen):
		 * 		* Log a warning or error because the message/passage will be
		 * 		  lost unless we can think of something sensible to do with it.
		 */

		logger.debug("Start processing passage: {}...", event.toString());
		//		try {
		//			logger.debug("Sleeping for 1000 ms to simulate doing some work...");
		//			Thread.sleep(1000);		// simulate doing some work
		//		} catch (InterruptedException e) {
		//		}
		logger.debug("Finished processing passage: {}...", event.toString());

		/*
		 * Send the result of the processing as a RabbitMQ message to a RabbitMQ
		 * broker.
		 * 
		 * First, create a RabbitMQ message payload.
		 */
		PassageTest1Protos.PassageTest1.Builder passage = PassageTest1Protos.PassageTest1.newBuilder();
		passage.setImageName(event.getImage_name());
		passage.setImage(ByteString.copyFrom(event.getImageBytes()));
		byte[] passageBytes = passage.build().toByteArray();

		/*
		 * Queue the message for publishing to a RabbitMQ broker. The actual
		 * publishing will be performed in a RabbitMQ producer background
		 * thread.
		 */
		logger.debug("Queuing message for delivery [{} bytes]", passageBytes.length);
		/*
		 * Remember that success here only means that the message was queued for
		 * delivery, not that is *was* delivered!
		 * TODO Should we try to follow up that the message *was* delivered successfully?
		 * If so, what should be done in that case?
		 */
		boolean success = this.send(passageBytes);
		if (success) {
			logger.debug("Message successfully queued.");
		} else {
			logger.warn("\n**********\nMessage could not be queued. It will be lost!\n**********");
		}

	}

	public boolean send(byte[] bytes) {

		BlockingQueue<byte[]> messageBlockingQueue = rabbitMQProducerController.getMessageBlockingQueue();

		logger.debug("messageBlockingQueue.size() = {}", messageBlockingQueue.size());
		logger.debug("messageBlockingQueue.remainingCapacity() = {}", messageBlockingQueue.remainingCapacity());

		/*    
		 * If the queue is not full this will enter the message into the queue 
		 * and then return "true". If the queue is full, this call will block 
		 * for up to a certain timeout period. If a queue entry becomes 
		 * available before this timeout period is reached, the message is 
		 * placed in the queue and "true" is returned; otherwise, "false" is 
		 * returned and the sender must deal with the failure.
		 * 
		 * TODO Should we deal with the failure here in send()? Look into this!
		 */
		logger.debug("Offering a message to the producer blocking queue [{} bytes]...", bytes.length);
		boolean success = false;
		//TODO This will block forever while the queue is full. Is this OK?
		while (!success) {
			try {
				//			success = messageBlockingQueue.offer(bytes);
				success = messageBlockingQueue.offer(bytes, PRODUCER_BLOCKING_QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				//TODO Can we ensure that the message to be queued, i.e., "bytes", is not lost?
				logger.warn("\n********************" +
						"\nInterruptedException caught while waiting to offer a message to the queue. " +
						"\nPerhaps a request has been made to stop the RabbitMQ producer thread(s)?" +
						"\nCan we ensure that the message to be queued is not lost?" +
						"\n********************");
			}
			if (success) {
				logger.debug("Message successfully entered into producer blocking queue.");
			} else {
				//TODO Can we ensure that the message to be queued, ie.e., "bytes", is not lost?
				logger.warn("\n**********\nMessage not entered into producer blocking queue. The queue is full!\n**********");
			}
		}
		return success;
	}

}
