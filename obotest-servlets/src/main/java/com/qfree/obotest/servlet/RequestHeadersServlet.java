package com.qfree.obotest.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet implementation class RequestHeadersServlet
 */
@WebServlet(description = "Displays all request headers", urlPatterns = { "/RequestHeaders" })
public class RequestHeadersServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(RequestHeadersServlet.class);

	/*
	 * EJBs can be injected with the @EJB annotation if this class is annotated
	 * with @WebServlet.
	 */
	//	@EJB
	//	RabbitMQConsumerController rabbitMQConsumerController;

	public RequestHeadersServlet() {
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

		response.setContentType("text/html;charset=UTF-8");
		// PrintWriter out = response.getWriter();
		try (PrintWriter out = response.getWriter()) {
			out.println("<html>");
			out.println("<head>");
			out.println("<title>Request Headers</title>");
			
			out.println("<style>");
			out.println("table {");
			out.println("width: 100%;");
			out.println("border-collapse: collapse;");
			out.println("}");
			out.println("table,th,td {");
			out.println("border:1px solid black;");
			out.println("}");
			out.println("td {");
			out.println("padding:2px;");
			out.println("}");
			out.println("</style>");

			out.println("</head>");
			out.println("<body>");
			out.println("<h3>Request headers at " + request.getContextPath()
					+ "</h3>");
			
			out.println("<table>");
			out.println("<tr>");
			out.println("<th>Header name</th>");
			out.println("<th>Header value</th>");
			out.println("</tr>");
			Enumeration<String> names = request.getHeaderNames();
			while (names.hasMoreElements()) {
				out.println("<tr>");
				String name = (String) names.nextElement();
				Enumeration<String> values = request.getHeaders(name); // support multiple values
				if (values != null) {
					
					String value;
					while (values.hasMoreElements()) {
						value = (String) values.nextElement();
						// out.println(name + ": " + value);
						out.println("<td width=\"20%\">" + name + "</td>");
						out.println("<td>" + value + "</td>");
						
						// If the header has multiple values, we display the 
						// header name for only the first value.
						while (values.hasMoreElements()) {
							value = (String) values.nextElement();
							// out.println(name + ": " + value);
							out.println("<td width=\"20%\"></td>");		// do not display the header name again
							out.println("<td>" + value + "</td>");
						}
						
					}
				} else {
					out.println("<td>" + name + "</td>");
					out.println("<td></td>");
				}
				out.println("</tr>");
			}

			out.println("</table>");
			out.println("</body>");
			out.println("</html>");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getServletInfo() {
		return "Returns all request headers formatted as an HTML table";
	}

}
