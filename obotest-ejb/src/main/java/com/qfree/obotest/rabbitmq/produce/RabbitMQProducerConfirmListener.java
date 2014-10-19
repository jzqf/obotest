package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.rabbitmq.client.ConfirmListener;

public class RabbitMQProducerConfirmListener implements ConfirmListener {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerConfirmListener.class);

	private final SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms;

	public RabbitMQProducerConfirmListener(SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms) {
		this.pendingPublisherConfirms = pendingPublisherConfirms;
	}

	@Override
	public void handleAck(long deliveryTag, boolean multiple) throws IOException {

		logger.debug("deliveryTag = {}, multiple = {}", deliveryTag, multiple);
		logger.debug("pendingPublisherConfirms BEFORE =\n{}", pendingPublisherConfirms);

		try {
			if (multiple) {
				/*
				 * Obtain a view into the pendingPublisherConfirms map that 
				 * contains all entries that the broker has just confirmed
				 * (acknowledged).
				 */
				SortedMap<Long, RabbitMQMsgAck> confirmedMessages = pendingPublisherConfirms.headMap(deliveryTag + 1);
				logger.debug("Multiple acks. confirmedMessages = {}", confirmedMessages);

				/*
				 * Ack the originally consumed messages that were processed to
				 * generate the messages that were subsequently published and 
				 * are now being confirmed by the broker that received them to 
				 * report that they were handled successfully.
				 * 
				 * In order to avoid a java.util.ConcurrentModificationException
				 * being thrown while iterating over confirmedMessages or 
				 * modifying it, it is *essential* that we synchronize this 
				 * block on the underlying map, pendingPublisherConfirms, not
				 * on confirmedMessages. Testing has shown that a 
				 * ConcurrentModificationException *will* be thrown without this
				 * synchronization. The result of this is that the producer
				 * thread in which this listener is installed (a 
				 * RabbitMQProducerRunnable object) will terminate. This will
				 * have two consequences:
				 * 
				 *   1. Delivery tags that are being managed in 
				 *      pendingPublisherConfirms will be lost and hence the 
				 *      original consumed messages whose delivery tags are 
				 *      stored in the RabbitMQMsgAck objects in this map will
				 *      never be acknowledged in the consumer threads. As a 
				 *      result, these messages will be requeued and consumed
				 *      again then the consumer threads are restarted. If all
				 *      messages are handled idempotently this may not have 
				 *      serious consequneces, but it is definitely not 
				 *      desirable.
				 *      
				 *   2. The producer thread that terminates because of the
				 *      ConcurrentModificationException will eventually be
				 *      restarted by RabbitMQProducerController.heartBeat(),
				 *      but this will not recover or be able to handle the lost
				 *      delivery tag data that is descried in the previous 
				 *      point.
				 */
				synchronized (pendingPublisherConfirms) { // Synchronize on pendingPublisherConfirms, not confirmedMessages!
					for (RabbitMQMsgAck rabbitMQMsgAck : confirmedMessages.values()) {	// ConcurrentModificationException exception here!!!!!
						logger.debug("Queuing ack for rabbitMQMsgAck: {}", rabbitMQMsgAck);
						rabbitMQMsgAck.queueAck();
					}

					/*
					 * Remove those elements just processed from the map
					 * "pendingPublisherConfirms".
					 */
					confirmedMessages.clear();
				}

			} else {

				/*
				 * Ack the originally consumed message that was processed to
				 * generate a message that was subsequently published and is 
				 * now being confirmed by the broker that received it was 
				 * handled successfully.
				 */
				logger.debug("Queuing ack for rabbitMQMsgAck: {}", pendingPublisherConfirms.get(deliveryTag));
				pendingPublisherConfirms.get(deliveryTag).queueAck();

				/*
				 * Remove the element just processed from the map
				 * "pendingPublisherConfirms".
				 */
				pendingPublisherConfirms.remove(deliveryTag);

			}

		} catch (Throwable e) {
			/*
			 * This is ugly because it will mess up the acknowledgement 
			 * mechanism for mode AFTER_PUBLISHED_CONFIRMED. The producer thread
			 * in which this listener is installed will crash. Although the
			 * thread will be restarted by RabbitMQProducerController.heartBeat(),
			 * the deliveryTags that were being managed in 
			 * pendingPublisherConfirms will be lost. This will result in 
			 * the corresponding messages not being acknowledged in the consumer
			 * threads. As a result, those messages will eventually be requeued
			 * when the consumer channels on which the messages were originally 
			 * consumed are closed.
			 */
			logger.error("\n\n**********************************************\n"
					+ "An exception was thrown in RabbitMQProducerConfirmListener!!!"
					+ "\n\n**********************************************\n", e);
		}
		logger.debug("pendingPublisherConfirms AFTER  =\n{}", pendingPublisherConfirms);

	}

	@Override
	public void handleNack(long deliveryTag, boolean multiple) throws IOException {

		logger.warn("deliveryTag = {}, multiple = {}", deliveryTag, multiple);
		logger.debug("pendingPublisherConfirms BEFORE =\n{}", pendingPublisherConfirms);

		if (multiple) {
			/*
			 * Obtain a view into the pendingPublisherConfirms map that 
			 * contains all entries that the broker has just confirmed
			 * (acknowledged).
			 */
			SortedMap<Long, RabbitMQMsgAck> confirmedMessages = pendingPublisherConfirms.headMap(deliveryTag + 1);

			/*
			 * Nack the originally consumed messages that were processed to
			 * generate the messages that were subsequently published and are 
			 * now being confirmed by the broker that received them to report
			 * that they were NOT handled successfully.
			 * 
			 * See method handleAck(...) for an explanation of why this 
			 * synchronized block is necessary.
			 */
			synchronized (pendingPublisherConfirms) { // Synchronize on pendingPublisherConfirms, not confirmedMessages!
				for (RabbitMQMsgAck rabbitMQMsgAck : confirmedMessages.values()) {
					logger.debug("Nacking rabbitMQMsgAck: {}", rabbitMQMsgAck);
					/*
					 * TODO Should the original messages be discarded/dead-lettered instead
					 *      of being requeued?
					 */
					rabbitMQMsgAck.queueNack(true);	// requeue message
				}

				/*
				 * Remove those elements just processed from the map
				 * "pendingPublisherConfirms".
				 */
				confirmedMessages.clear();
			}

		} else {

			/*
			 * Nack the originally consumed message that was processed to
			 * generate a message that was subsequently published and is 
			 * now being confirmed by the broker that received it that it 
			 * was NOT handled successfully.
			 */
			logger.debug("Nacking rabbitMQMsgAck: {}", pendingPublisherConfirms.get(deliveryTag));
			/*
			 * TODO Should the original messages be discarded/dead-lettered instead
			 *      of being requeued?
			 */
			pendingPublisherConfirms.get(deliveryTag).queueNack(true);	// requeue message

			/*
			 * Remove the element just processed from the map
			 * "pendingPublisherConfirms".
			 */
			pendingPublisherConfirms.remove(deliveryTag);

		}

		logger.debug("pendingPublisherConfirms AFTER  =\n{}", pendingPublisherConfirms);

	}

}
