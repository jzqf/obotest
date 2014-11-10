package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;

@Path("/rabbitmq/unserviced_async_calls")
public class RabbitMQMonitorUnservicedAsyncCallsController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorUnservicedAsyncCallsController.class);

	@GET
	@Path("/num")
	@Produces("text/plain;v=1")
	public int num() {
		return RabbitMQConsumerController.UNSERVICED_ASYNC_CALLS_MAX
				- RabbitMQConsumerController.unservicedAsyncCallsCounterSemaphore.availablePermits();
	}

	@GET
	@Path("/max")
	@Produces("text/plain;v=1")
	public int max() {
		return RabbitMQConsumerController.UNSERVICED_ASYNC_CALLS_MAX;
	}

}
