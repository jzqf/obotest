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

@WebServlet(description = "Enables the RabbitMQ consumer thread", urlPatterns = { "/enable_rabbitmq_consumer" })
public class RabbitMQEnableConsumerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQEnableConsumerServlet.class);

	@EJB
	RabbitMQConsumerController rabbitMQConsumerController;

	public RabbitMQEnableConsumerServlet() {
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

		/*
		 * Note that this will start the consumer threads if they are not
		 * already running.
		 */
		logger.debug("Calling rabbitMQConsumerController.enable()...");
		rabbitMQConsumerController.enable();
		//		logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING...");
		//		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;

		response.setContentType("text/html;charset=UTF-8");
		// PrintWriter out = response.getWriter();
		try (PrintWriter out = response.getWriter()) {
			out.println("<html>");
			out.println("<head>");
			out.println("<title>RabbitMQ</title>");
			out.println("</head>");
			out.println("<body>");
			out.println("<h3>RabbitMQ consumer signalled to enable itself</h3>");
			out.println("</body>");
			out.println("</html>");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getServletInfo() {
		return "Signals the RabbitMQ consumer thread to enable itself";
	}

}
