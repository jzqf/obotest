package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;

@Path("/rabbitmq/message_handlers")
public class RabbitMQMonitorMessageHandlersController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorMessageHandlersController.class);

	@GET
	@Path("/num")
	@Produces("text/plain;v=1")
	public int num() {
		return RabbitMQConsumerController.MAX_MESSAGE_HANDLERS
				- RabbitMQConsumerController.messageHandlerCounterSemaphore.availablePermits();
	}

	@GET
	@Path("/max")
	@Produces("text/plain;v=1")
	public int max() {
		return RabbitMQConsumerController.MAX_MESSAGE_HANDLERS;
	}

}
