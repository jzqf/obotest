package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;

@Path("/rabbitmq/unacknowledged_message_handler_calls")
public class RabbitMQMonitorUnackedMessageHandlerCallssController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorUnackedMessageHandlerCallssController.class);

	@GET
	@Path("/num")
	@Produces("text/plain;v=1")
	public int num() {
		return RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS
				- RabbitMQConsumerController.unacknowledgeCDIEventsCounterSemaphore.availablePermits();
	}

	@GET
	@Path("/max")
	@Produces("text/plain;v=1")
	public int max() {
		return RabbitMQConsumerController.MAX_UNACKNOWLEDGED_CDI_EVENTS;
	}

}
