package com.qfree.obotest.rest.monitoring.rabbitmq;

import java.util.List;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerRunnable;

@Path("/rabbitmq/publishers")
@RequestScoped
public class RabbitMQMonitorPublishersController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQMonitorPublishersController.class);

	@Inject
	RabbitMQProducerController rabbitMQProducerController;

	@GET
	@Produces("text/plain;v=1")
	public int numPublishers() {
		return RabbitMQProducerController.NUM_RABBITMQ_PRODUCER_THREADS;
	}

	@GET
	@Path("/{thread}/pending_publisher_acks")
	@Produces("text/plain;v=1")
	public int pending_publisher_acks(@PathParam("thread") int thread) {
		
		logger.info("/pending_publisher_acks requested for publisher thread #{}", thread);

		int pending_publisher_acks = -1;

		RabbitMQProducerRunnable rabbitMQProducerRunnable = getRabbitMQProducerRunnable(thread - 1);
		if (rabbitMQProducerRunnable != null) {
			pending_publisher_acks = rabbitMQProducerRunnable.getPendingPublisherConfirms().size();
		} else {
			/*
			 * This case can occur if this code is called before the 
			 * publisher threads have been started.
			 */
		}
		return pending_publisher_acks;
	}

	/**
	 * Returns the RabbitMQProducerRunnable associated with index "threadIndex",
	 * or null if it does not exist 
	 * 
	 * @param threadIndex
	 */
	private RabbitMQProducerRunnable getRabbitMQProducerRunnable(int threadIndex) {

		logger.info("RabbitMQProducerRunnable requested for thread index #{}", threadIndex);

		RabbitMQProducerRunnable rabbitMQProducerRunnable = null;

		if (RabbitMQProducerController.NUM_RABBITMQ_PRODUCER_THREADS == 1) {

			if (threadIndex == 0) {
				rabbitMQProducerRunnable = rabbitMQProducerController.getRabbitMQProducerRunnable();
			} else {
				logger.error("threadIndex = {}. The only legal value here is 0.", threadIndex);
			}

		} else {

			List<RabbitMQProducerRunnable> rabbitMQProducerRunnables =
					rabbitMQProducerController.getRabbitMQProducerRunnables();
			if (rabbitMQProducerRunnables != null) {
				if ((threadIndex < rabbitMQProducerRunnables.size()) && (threadIndex >= 0)) {
					rabbitMQProducerRunnable = rabbitMQProducerRunnables.get(threadIndex);
				} else {
					logger.error("threadIndex = {}. Value must be in range [0, {}].", threadIndex,
							RabbitMQProducerController.NUM_RABBITMQ_PRODUCER_THREADS - 1);
				}
			} else {
				logger.error("rabbitMQProducerController.getRabbitMQProducerRunnables() is null");
			}

		}

		return rabbitMQProducerRunnable;
	}

}
