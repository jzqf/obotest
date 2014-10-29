package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;

@Path("/rabbitmq/publisher_queue")
public class RabbitMQMonitorPublisherQueueController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorPublisherQueueController.class);

	@GET
	@Path("/capacity")
	@Produces("text/plain;v=1")
	public int capacity() {
		return RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH;
	}

	@GET
	@Path("/size")
	@Produces("text/plain;v=1")
	public int size() {
		return RabbitMQProducerController.producerMsgQueue.size();
	}

	@GET
	@Path("/percent_full")
	@Produces("text/plain;v=1")
	public int percent_full() {
		return Math.round(
				(float) RabbitMQProducerController.producerMsgQueue.size() /
				(float) RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH
				* 100);
	}

}
