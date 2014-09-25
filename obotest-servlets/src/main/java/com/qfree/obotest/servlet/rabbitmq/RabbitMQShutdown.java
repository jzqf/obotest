package com.qfree.obotest.servlet.rabbitmq;

import java.io.IOException;
import java.io.PrintWriter;

import javax.ejb.EJB;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;

@WebServlet(description = "Shuts down both the RabbitMQ consumer and producer threads in an orderly fashion",
		urlPatterns = { "/shutdown_rabbitmq" })
public class RabbitMQShutdown extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQShutdown.class);

	@EJB
	RabbitMQConsumerController rabbitMQConsumerController;

	@EJB
	RabbitMQProducerController rabbitMQProducerController;

	public RabbitMQShutdown() {
		super();
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		try {
			processRequest(request, response);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		try {
			processRequest(request, response);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {

		logger.debug("rabbitMQConsumerController.acquiredMessageHandlerPermits() = {}",
				rabbitMQConsumerController.acquiredMessageHandlerPermits());

		logger.debug("Calling rabbitMQConsumerController.stopConsumerThreadsAndWaitForTermination()...");
		rabbitMQConsumerController.stopConsumerThreadsAndWaitForTermination();

		//		logger.debug("rabbitMQConsumerController.acquiredMessageHandlerPermits() = {}",
		//				rabbitMQConsumerController.acquiredMessageHandlerPermits());
		//		logger.debug("Sleeping 0 seconds...");
		//		long longSleep = 2000;
		//		try {
		//			Thread.sleep(longSleep);
		//		} catch (InterruptedException e) {
		//		}
		logger.debug("rabbitMQConsumerController.acquiredMessageHandlerPermits() = {}",
				rabbitMQConsumerController.acquiredMessageHandlerPermits());

		logger.debug("Calling rabbitMQProducerController.waitForIncomingMessageHandlerThreadsToFinish()...");
		rabbitMQProducerController.waitForIncomingMessageHandlerThreadsToFinish();	//TODO Check if this is thread-safe or if we need "volatile"

		logger.debug("rabbitMQConsumerController.acquiredMessageHandlerPermits() = {}",
				rabbitMQConsumerController.acquiredMessageHandlerPermits());

		logger.debug("{} elements left in messageBlockingQueue.",
				RabbitMQProducerController.messageBlockingQueue.size());

		logger.debug("\n\nWaiting for the messageBlockingQueue queue to empty...");
		rabbitMQProducerController.waitForRabbitMQProducerQueueToEmpty();

		logger.info("\n\nStopping the RabbitMQ producer threads...");
		rabbitMQProducerController.stopProducerThreadsAndWaitForTermination();

//		rabbitMQProducerController.shutdown();

		response.setContentType("text/html;charset=UTF-8");
		// PrintWriter out = response.getWriter();
		try (PrintWriter out = response.getWriter()) {
			out.println("<html>");
			out.println("<head>");
			out.println("<title>RabbitMQ</title>");
			out.println("</head>");
			out.println("<body>");
			out.println("<h3>The RabbitMQ interface has been shut down. It is now safe to shutdown the application container.</h3>");
			out.println("</body>");
			out.println("</html>");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getServletInfo() {
		return "Signals the RabbitMQ consumer and producer threads to stop in an orderly fashion";
	}

}
