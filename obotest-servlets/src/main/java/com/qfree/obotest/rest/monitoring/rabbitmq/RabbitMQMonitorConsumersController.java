package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;

@Path("/rabbitmq/consumers")
@RequestScoped
public class RabbitMQMonitorConsumersController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorConsumersController.class);

	@Inject
	RabbitMQConsumerController rabbitMQConsumerController;

	@GET
	@Produces("text/plain;v=1")
	public int numConsumers() {
		return RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS;
	}

	@GET
	@Path("/{thread}/ack_queue/capacity")
	@Produces("text/plain;v=1")
	public int ack_queue_capacity(@PathParam("thread") int thread) {
		
		logger.info("Request for /ack_queue/capacity for consumer thread #{}", thread);

		int ack_queue_capacity = -1;
		//		if (RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {
		//
		//			if (rabbitMQConsumerController.rabbitMQConsumerRunnable != null) {
		//				//TODO Write me!!!!!!!!!!!!!!!
		//			} else {
		//				logger.error("rabbitMQConsumerController.rabbitMQConsumerRunnable is null. thread = {}", thread);
		//			}
		//
		//		} else {
		//
		//			if (rabbitMQConsumerController.rabbitMQConsumerRunnables != null) {
		//				if (rabbitMQConsumerController.rabbitMQConsumerRunnables.size() >= thread) {
		//					/* 
		//					 * "threadIndex" is zero-based, but "thread" is one-based.
		//					 */
		//					int threadIndex = thread - 1;
		//					//TODO Write me!!!!!!!!!!!!!!!
		//				} else {
		//					//TODO Write me!!!!!!!!!!!!!!!
		//				}
		//			} else {
		//				logger.error("rabbitMQConsumerController.rabbitMQConsumerRunnables is null. thread = {}", thread);
		//			}
		//
		//		}
		return ack_queue_capacity;
	}

}
