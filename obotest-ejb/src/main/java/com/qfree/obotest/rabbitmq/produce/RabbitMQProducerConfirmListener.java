package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.RabbitMQMsgAck;
import com.rabbitmq.client.ConfirmListener;

public class RabbitMQProducerConfirmListener implements ConfirmListener {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerConfirmListener.class);

	private SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms;

	public RabbitMQProducerConfirmListener(SortedMap<Long, RabbitMQMsgAck> pendingPublisherConfirms) {
		this.pendingPublisherConfirms = pendingPublisherConfirms;
	}

	@Override
	public void handleAck(long deliveryTag, boolean multiple) throws IOException {

		logger.debug("deliveryTag = {}, multiple = {}", deliveryTag, multiple);

		logger.debug("pendingPublisherConfirms BEFORE =\n{}", pendingPublisherConfirms);

		if (multiple) {
			/*
			 * Obtain a view into the pendingPublisherConfirms map that 
			 * contains all entries that the broker has just confirmed
			 * (acknowledged).
			 */
			SortedMap<Long, RabbitMQMsgAck> confirmedMessages = pendingPublisherConfirms.headMap(deliveryTag + 1);

			/*
			 * Ack the originally consumed messages that were processed to
			 * generate the messages that were subsequently published and are 
			 * now being confirmed by the broker that received them that they 
			 * were handled successfully.
			 */
			for (RabbitMQMsgAck rabbitMQMsgAck : confirmedMessages.values()) {
				logger.debug("Acking rabbitMQMsgAck: {}", rabbitMQMsgAck);
				rabbitMQMsgAck.queueAck();
			}

			/*
			 * Remove those elements just processed from the map
			 * "pendingPublisherConfirms".
			 */
			confirmedMessages.clear();

		} else {

			/*
			 * Ack the originally consumed message that was processed to
			 * generate a message that was subsequently published and is 
			 * now being confirmed by the broker that received it that it 
			 * was handled successfully.
			 */
			logger.debug("Acking rabbitMQMsgAck: {}", pendingPublisherConfirms.get(deliveryTag));
			pendingPublisherConfirms.get(deliveryTag).queueAck();

			/*
			 * Remove the element just processed from the map
			 * "pendingPublisherConfirms".
			 */
			pendingPublisherConfirms.remove(deliveryTag);

		}

		logger.debug("pendingPublisherConfirms AFTER  =\n{}", pendingPublisherConfirms);

	}

	@Override
	public void handleNack(long deliveryTag, boolean multiple) throws IOException {

		logger.debug("deliveryTag = {}, multiple = {}", deliveryTag, multiple);

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
			 * now being confirmed by the broker that received them that they 
			 * were NOT handled successfully.
			 */
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
