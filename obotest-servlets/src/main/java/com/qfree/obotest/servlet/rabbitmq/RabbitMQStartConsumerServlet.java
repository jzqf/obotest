package com.qfree.obotest.servlet.rabbitmq;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController;
import com.qfree.obotest.rabbitmq.consume.RabbitMQConsumerController.RabbitMQConsumerControllerStates;

@WebServlet(description = "Starts the RabbitMQ consumer thread", urlPatterns = { "/start_rabbitmq_consumer" })
public class RabbitMQStartConsumerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQStartConsumerServlet.class);

	//	@EJB
	//	RabbitMQConsumerController rabbitMQConsumerController;

	public RabbitMQStartConsumerServlet() {
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

		//		logger.debug("Calling rabbitMQConsumerController.start()...");
		//		rabbitMQConsumerController.start();
		logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING...");
		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.RUNNING;

		response.setContentType("text/html;charset=UTF-8");
		// PrintWriter out = response.getWriter();
		try (PrintWriter out = response.getWriter()) {
			out.println("<html>");
			out.println("<head>");
			out.println("<title>RabbitMQ</title>");
			out.println("</head>");
			out.println("<body>");
			out.println("<h3>RabbitMQ consumer signalled to start</h3>");
			out.println("</body>");
			out.println("</html>");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getServletInfo() {
		return "Signals the RabbitMQ consumer thread to start";
	}

}
