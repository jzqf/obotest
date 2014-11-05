package com.qfree.obotest.rest.monitoring.rabbitmq;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerRunnable;

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
	@Path("/state")
	@Produces("text/plain;v=1")
	public String state() {
		return RabbitMQConsumerController.state.toString();
	}

	@GET
	@Path("/{thread}/ack_queue/{subCommand}")
	@Produces("text/plain;v=1")
	public int ack_queue_size_v1(@PathParam("thread") int thread,
			@PathParam("subCommand") String subCommand,
			@HeaderParam("Accept") String acceptHeader) {
		
		//logger.info("/ack_queue/size requested for consumer thread #{}", thread);
		//logger.info("acceptHeader = {}", acceptHeader);

		int ack_queue_value = -1;

		if (subCommand.equals("size")) {

			RabbitMQConsumerRunnable rabbitMQConsumerRunnable = getRabbitMQConsumerRunnable(thread - 1);
			if (rabbitMQConsumerRunnable != null) {
				ack_queue_value = rabbitMQConsumerRunnable.getAcknowledgementQueue().size();
			} else {
				/*
				 * This case can occur if this code is called before the 
				 * consumer threads have been started.
				 */
			}

		} else if (subCommand.equals("capacity")) {

			ack_queue_value = RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH;  // independent of "thread"

		}
		return ack_queue_value;
	}

	//	@GET
	//	@Path("/{thread}/ack_queue/capacity")
	//	@Produces("text/plain;v=1")
	//	public int ack_queue_capacity(@PathParam("thread") int thread) {
	//		return RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH;  // independent of "thread"
	//	}

	@GET
	@Path("/{thread}/throttled")
	@Produces("text/plain;v=1")
	public int throttled(@PathParam("thread") int thread) {

		//logger.info("/throttled requested for consumer thread #{}", thread);

		boolean throttled = false;

		RabbitMQConsumerRunnable rabbitMQConsumerRunnable = getRabbitMQConsumerRunnable(thread - 1);
		if (rabbitMQConsumerRunnable != null) {
			throttled = rabbitMQConsumerRunnable.isThrottled();
		} else {
			/*
			 * This case can occur if this code is called before the 
			 * consumer threads have been started.
			 */
		}
		return throttled ? 1 : 0;
	}

	@GET
	@Path("/{thread}/throttled_publisher_msg_queue")
	@Produces("text/plain;v=1")
	public int throttled_publisher_msg_queue(@PathParam("thread") int thread) {

		//logger.info("/throttled_publisher_msg_queue requested for consumer thread #{}", thread);

		boolean throttled_producer_msg_queue = false;

		RabbitMQConsumerRunnable rabbitMQConsumerRunnable = getRabbitMQConsumerRunnable(thread - 1);
		if (rabbitMQConsumerRunnable != null) {
			throttled_producer_msg_queue = rabbitMQConsumerRunnable.isThrottled_ProducerMsgQueue();
		} else {
			/*
			 * This case can occur if this code is called before the 
			 * consumer threads have been started.
			 */
		}
		return throttled_producer_msg_queue ? 1 : 0;
	}

	@GET
	@Path("/{thread}/throttled_unacked_async_calls")
	@Produces("text/plain;v=1")
	public int throttled_unacked_async_calls(@PathParam("thread") int thread) {

		//logger.info("/throttled_unacked_async_calls requested for consumer thread #{}", thread);

		boolean throttled_unacked_async_calls = false;

		RabbitMQConsumerRunnable rabbitMQConsumerRunnable = getRabbitMQConsumerRunnable(thread - 1);
		if (rabbitMQConsumerRunnable != null) {
			throttled_unacked_async_calls = rabbitMQConsumerRunnable.isThrottled_UnacknowledgedCDIEvents();
		} else {
			/*
			 * This case can occur if this code is called before the 
			 * consumer threads have been started.
			 */
		}
		logger.info(
				"/throttled_unacked_async_calls requested for consumer thread #{}.\n    throttled_unacked_async_calls={}, (throttled_unacked_async_calls ? 1 : 0)={}",
				thread, throttled_unacked_async_calls, throttled_unacked_async_calls ? 1 : 0);
		return throttled_unacked_async_calls ? 1 : 0;
	}

	/**
	 * Returns the RabbitMQConsumerRunnable associated with index "threadIndex",
	 * or null if it does not exist 
	 * 
	 * @param threadIndex
	 */
	private RabbitMQConsumerRunnable getRabbitMQConsumerRunnable(int threadIndex) {

		//logger.info("RabbitMQConsumerRunnable requested for thread index #{}", threadIndex);

		RabbitMQConsumerRunnable rabbitMQConsumerRunnable = null;

		if (RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {

			if (threadIndex == 0) {
				rabbitMQConsumerRunnable = RabbitMQConsumerController.rabbitMQConsumerRunnable;
			} else {
				//logger.error("threadIndex = {}. The only legal value here is 0.", threadIndex);
			}

		} else {

			if (RabbitMQConsumerController.rabbitMQConsumerRunnables != null) {
				if ((threadIndex < RabbitMQConsumerController.rabbitMQConsumerRunnables.size()) && (threadIndex >= 0)) {
					rabbitMQConsumerRunnable = RabbitMQConsumerController.rabbitMQConsumerRunnables.get(threadIndex);
				} else {
					logger.error("threadIndex = {}. Value must be in range [0, {}].", threadIndex,
							RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS - 1);
				}
			} else {
				logger.error("rabbitMQConsumerController.rabbitMQConsumerRunnables is null");
			}

		}

		return rabbitMQConsumerRunnable;
	}

}
