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
		if (RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS == 1) {

			if (RabbitMQConsumerController.rabbitMQConsumerRunnable != null) {
				ack_queue_size = RabbitMQConsumerController.rabbitMQConsumerRunnable.getAcknowledgementQueue().size();
			} else {
				/*
				 * This case will occur if this was called before the consumer  
				 * threads have been started. We return a sensible value so that 
				 * monitoring will still function sensibly.
				 */
				ack_queue_size = 0;
			}

		} else {

			if (RabbitMQConsumerController.rabbitMQConsumerRunnables != null) {
				if ((thread <= RabbitMQConsumerController.rabbitMQConsumerRunnables.size()) && (thread >= 1)) {
					/* 
					 * "threadIndex" is zero-based, but "thread" is one-based.
					 */
					int threadIndex = thread - 1;
					RabbitMQConsumerRunnable rabbitMQConsumerRunnable =
							RabbitMQConsumerController.rabbitMQConsumerRunnables.get(threadIndex);
					if (rabbitMQConsumerRunnable != null) {
					ack_queue_size = rabbitMQConsumerRunnable.getAcknowledgementQueue().size();
					} else {
						/*
						 * This case will occur if this was called before the consumer  
						 * threads have been started. We return a sensible value so that 
						 * monitoring will still function sensibly.
						 */
						ack_queue_size = 0;
					}
				} else {
					logger.error("thread = {}. Value must be in range [0,{}].", thread,
							RabbitMQConsumerController.NUM_RABBITMQ_CONSUMER_THREADS);
				}
			} else {
				logger.error("rabbitMQConsumerController.rabbitMQConsumerRunnables is null. thread = {}", thread);
			}

		}
		return ack_queue_size;
	}

	@GET
	@Path("/{thread}/ack_queue/capacity")
	@Produces("text/plain;v=1")
	public int ack_queue_capacity(@PathParam("thread") int thread) {
		return RabbitMQConsumerController.ACKNOWLEDGEMENT_QUEUE_LENGTH;  // independent of "thread"
	}

	//	@GET
	//	@Path("/{thread}/throttled")
	//	@Produces("text/plain;v=1")
	//	public int throttled(@PathParam("thread") int thread) {
	//		return ;
	//	}

}
