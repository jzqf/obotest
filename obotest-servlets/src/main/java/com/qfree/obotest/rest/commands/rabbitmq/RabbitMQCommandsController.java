package com.qfree.obotest.rest.commands.rabbitmq;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;

@Path("/rabbitmq/commands")
/*
 * This class must be annotated as an EJB session bean in order for it to be
 * able to use the @EJB annotation below for dependency injection...
 */
//@Stateless
/*
 * ... Or it can be annotated with @RequestScoped and then the CDI @Inject 
 * annotation can be used for dependency injection.
 */
@RequestScoped
public class RabbitMQCommandsController {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQCommandsController.class);

	//	@EJB
	@Inject
	RabbitMQConsumerController rabbitMQConsumerController;

	//	@EJB
	@Inject
	RabbitMQProducerController rabbitMQProducerController;

	@GET
	@Path("/start")
	@Produces("text/plain;v=1")
	public String start() {

		logger.info("Calling rabbitMQProducerController.start()...");
		rabbitMQProducerController.start();

		logger.info("Calling rabbitMQConsumerController.start()...");
		rabbitMQConsumerController.start();

		return "RabbitMQ consumer and producer signalled to start";
	}

	@GET
	@Path("/stop")
	@Produces("text/plain;v=1")
	public String stop() {

		logger.info("Calling rabbitMQProducerController.shutdown()...");
		rabbitMQProducerController.shutdown();

		return "The RabbitMQ consumer and producer have been shut down."
				+ " It is now safe to shutdown the application container.";
	}

}
