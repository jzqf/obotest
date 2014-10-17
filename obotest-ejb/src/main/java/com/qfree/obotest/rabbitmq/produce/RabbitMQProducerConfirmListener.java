package com.qfree.obotest.rabbitmq.produce;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConfirmListener;

public class RabbitMQProducerConfirmListener implements ConfirmListener {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerConfirmListener.class);

	@Override
	public void handleAck(long deliveryTag, boolean multiple) throws IOException {

		logger.info("deliveryTag = {}, multiple = {}", deliveryTag, multiple);

	}

	@Override
	public void handleNack(long deliveryTag, boolean multiple) throws IOException {

		logger.info("deliveryTag = {}, multiple = {}", deliveryTag, multiple);

	}

}
