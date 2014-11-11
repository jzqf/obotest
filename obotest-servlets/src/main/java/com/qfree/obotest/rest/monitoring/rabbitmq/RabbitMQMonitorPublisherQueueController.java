package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.qfree.obotest.rest.ReST;

@Path("/rabbitmq/publisher_queue")
public class RabbitMQMonitorPublisherQueueController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorPublisherQueueController.class);

	@GET
	@Path("/capacity")
	@Produces("text/plain")
	public int capacity(@HeaderParam("Accept") String acceptHeader) {
		String apiVersion = ReST.extractAPIVersion(acceptHeader);
		return RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH;
	}

	@GET
	@Path("/size")
	@Produces("text/plain")
	public int size(@HeaderParam("Accept") String acceptHeader) {
		String apiVersion = ReST.extractAPIVersion(acceptHeader);
		return RabbitMQProducerController.producerMsgQueue.size();
	}

	@GET
	@Path("/percent_full")
	@Produces("text/plain")
	public int percent_full(@HeaderParam("Accept") String acceptHeader) {
		String apiVersion = ReST.extractAPIVersion(acceptHeader);
		return Math.round(
				(float) RabbitMQProducerController.producerMsgQueue.size() /
				(float) RabbitMQProducerController.PRODUCER_BLOCKING_QUEUE_LENGTH
				* 100);
	}

}
