package com.qfree.obotest.rest.commands.rabbitmq;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerControllerStates;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController.RabbitMQProducerControllerStates;

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

		return "RabbitMQ consumer and publisher signalled to start";
	}

	@GET
	@Path("/stop")
	@Produces("text/plain;v=1")
	public String stop() {

		logger.info("Calling rabbitMQProducerController.shutdown()...");
		rabbitMQProducerController.shutdown();

		return "The RabbitMQ consumer and publisher have been shut down."
				+ " It is now safe to shutdown the application container.";
	}

	@GET
	@Path("/consumers/{subCommand}")
	@Produces("text/plain;v=1")
	public String executeConsumerCommand(@PathParam("subCommand") String subCommand) {

		String returnMsg = "";

		if (subCommand.equals("disable")) {

			logger.info("Calling rabbitMQConsumerController.disable()...");
			rabbitMQConsumerController.disable();
			//		logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.DISABLED...");
			//		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.DISABLED;
			returnMsg = "RabbitMQ consumer signalled to disable itself";

		} else if (subCommand.equals("enable")) {

			logger.info("Calling rabbitMQConsumerController.enable()...");
			rabbitMQConsumerController.enable();
			//		logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING...");
			//		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;
			returnMsg = "RabbitMQ consumer signalled to enable itself";

		} else if (subCommand.equals("start")) {

			logger.info("Calling rabbitMQConsumerController.start()...");
			rabbitMQConsumerController.start();
			//		logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING...");
			//		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;
			returnMsg = "RabbitMQ consumer signalled to start";

		} else if (subCommand.equals("stop")) {

			//		logger.info("Calling rabbitMQConsumerController.stop()...");
			//		rabbitMQConsumerController.stop();
			logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED...");
			RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.STOPPED;
			returnMsg = "RabbitMQ consumer signalled to stop";

		} else if (subCommand.equals("state")) {

			returnMsg = RabbitMQConsumerController.state.toString();

		} else {
			returnMsg = "Unknown subcomand: " + subCommand;
		}
		return returnMsg;
	}

	@GET
	@Path("/publishers/{subCommand}")
	@Produces("text/plain;v=1")
	public String executeProducerCommand(@PathParam("subCommand") String subCommand) {

		String returnMsg = "";

		if (subCommand.equals("start")) {

			logger.debug("Calling rabbitMQProducerController.start()...");
			rabbitMQProducerController.start();
			//		logger.info("Setting RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING...");
			//		RabbitMQProducerController.state = RabbitMQProducerControllerStates.RUNNING;
			returnMsg = "RabbitMQ publisher signalled to start";

		} else if (subCommand.equals("stop")) {

			//		logger.debug("Calling rabbitMQProducerController.stop()...");
			//		rabbitMQProducerController.stop();
			logger.info("Setting RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED...");
			RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;
			returnMsg = "RabbitMQ publisher signalled to stop";

		} else if (subCommand.equals("state")) {

			returnMsg = RabbitMQProducerController.state.toString();

		} else {
			returnMsg = "Unknown subcomand: " + subCommand;
		}
		return returnMsg;
	}

}
