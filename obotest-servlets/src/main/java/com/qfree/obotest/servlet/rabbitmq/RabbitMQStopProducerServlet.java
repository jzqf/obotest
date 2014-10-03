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

import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController;
import com.qfree.obotest.rabbitmq.produce.RabbitMQProducerController.RabbitMQProducerControllerStates;

@WebServlet(description = "Stops the RabbitMQ producer thread", urlPatterns = { "/stop_rabbitmq_producer" })
public class RabbitMQStopProducerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQStopProducerServlet.class);

	//	@EJB
	//	RabbitMQProducerController rabbitMQProducerController;

	public RabbitMQStopProducerServlet() {
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

		//		logger.debug("Calling rabbitMQProducerController.stop()...");
		//		rabbitMQProducerController.stop();
		logger.info("Setting RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED...");
		RabbitMQProducerController.state = RabbitMQProducerControllerStates.STOPPED;

		response.setContentType("text/html;charset=UTF-8");
		// PrintWriter out = response.getWriter();
		try (PrintWriter out = response.getWriter()) {
			out.println("<html>");
			out.println("<head>");
			out.println("<title>RabbitMQ</title>");
			out.println("</head>");
			out.println("<body>");
			out.println("<h3>RabbitMQ producer signalled to stop</h3>");
			out.println("</body>");
			out.println("</html>");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getServletInfo() {
		return "Signals the RabbitMQ producer thread to stop";
	}

}
