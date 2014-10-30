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
	@Path("/{thread}/ack_queue/size")
	@Produces("text/plain;v=1")
	public int ack_queue_size(@PathParam("thread") int thread) {
		
		logger.info("/ack_queue/size requested for consumer thread #{}", thread);

		int ack_queue_size = -1;

		RabbitMQConsumerRunnable rabbitMQConsumerRunnable = getRabbitMQConsumerRunnable(thread - 1);
		if (rabbitMQConsumerRunnable != null) {
			ack_queue_size = rabbitMQConsumerRunnable.getAcknowledgementQueue().size();
		} else {
			/*
			 * This case can occur if this code is called before the 
			 * consumer threads have been started.
			 */
		}
		return ack_queue_size;
	}

	@GET
	@Path("/{thread}/ack_queue/capacity")
	@Produces("text/plain;v=1")
	public int ack_queue_capacity(@PathParam("thread") int thread) {
		return RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH;  // independent of "thread"
	}

	@GET
	@Path("/{thread}/throttled")
	@Produces("text/plain;v=1")
	public int throttled(@PathParam("thread") int thread) {

		logger.info("/throttled requested for consumer thread #{}", thread);

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
	@Path("/{thread}/throttled_producer_msg_queue")
	@Produces("text/plain;v=1")
	public int throttled_producer_msg_queue(@PathParam("thread") int thread) {

		logger.info("/throttled_producer_msg_queue requested for consumer thread #{}", thread);

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

		logger.info("/throttled_unacked_async_calls requested for consumer thread #{}", thread);

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
		return throttled_unacked_async_calls ? 1 : 0;
	}

	/**
	 * Returns the RabbitMQConsumerRunnable associated with index "threadIndex",
	 * or null if it does not exist 
	 * 
	 * @param threadIndex
	 */
	private RabbitMQConsumerRunnable getRabbitMQConsumerRunnable(int threadIndex) {

		logger.info("RabbitMQConsumerRunnable requested for thread index #{}", threadIndex);

		RabbitMQConsumerRunnable rabbitMQConsumerRunnable = null;

		if (RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {

			if (threadIndex == 0) {
				rabbitMQConsumerRunnable = RabbitMQConsumerController.rabbitMQConsumerRunnable;
			} else {
				logger.error("threadIndex = {}. The only legal value here is 0.", threadIndex);
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
