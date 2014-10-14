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

@WebServlet(description = "Disables the RabbitMQ consumer thread", urlPatterns = { "/disable_rabbitmq_consumer" })
public class RabbitMQDisableConsumerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQDisableConsumerServlet.class);

	@EJB
	RabbitMQConsumerController rabbitMQConsumerController;

	public RabbitMQDisableConsumerServlet() {
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

		logger.debug("Calling rabbitMQConsumerController.disable()...");
		rabbitMQConsumerController.disable();
		//		logger.info("Setting RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.DISABLED...");
		//		RabbitMQConsumerController.state = RabbitMQConsumerControllerStates.DISABLED;

		response.setContentType("text/html;charset=UTF-8");
		// PrintWriter out = response.getWriter();
		try (PrintWriter out = response.getWriter()) {
			out.println("<html>");
			out.println("<head>");
			out.println("<title>RabbitMQ</title>");
			out.println("</head>");
			out.println("<body>");
			out.println("<h3>RabbitMQ consumer signalled to disable itself</h3>");
			out.println("</body>");
			out.println("</html>");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getServletInfo() {
		return "Signals the RabbitMQ consumer thread to disable itself";
	}

}
