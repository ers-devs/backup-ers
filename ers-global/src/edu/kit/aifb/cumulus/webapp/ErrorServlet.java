package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.logging.Logger;

/** 
 * 
 * @author aharth
 */
@SuppressWarnings("serial")
public class ErrorServlet extends HttpServlet {

	private final Logger _log = Logger.getLogger(this.getClass().getName());
		
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		PrintWriter out = resp.getWriter();
		
		String accept = req.getHeader("Accept");
		if (accept != null && accept.contains("html")) {			
			accept = "text/html";
		} else {
			accept = "text/plain";			
		}
		String code = null, message = null, type = null, uri = null;
		Object codeObj, messageObj, typeObj;
		Throwable throwable;
	    
	    // Retrieve the three possible error attributes, some may be null
	    codeObj = req.getAttribute("javax.servlet.error.status_code");
	    messageObj = req.getAttribute("javax.servlet.error.message");
	    typeObj = req.getAttribute("javax.servlet.error.exception_type");
	    throwable = (Throwable) req.getAttribute("javax.servlet.error.exception");
	    uri = (String) req.getAttribute("javax.servlet.error.request_uri");

	    if (uri == null) {
	      uri = req.getRequestURI(); // in case there's no URI given
	    }
	    // Convert the attributes to string values
	    // We do things this way because some old servers return String
	    // types while new servers return Integer, String, and Class types.
	    // This works for all.
	    if (codeObj != null) code = codeObj.toString();
	    if (messageObj != null) message = messageObj.toString();
	    if (typeObj != null) type = typeObj.toString();

	    // The error reason is either the status code or exception type
	    String reason = (code != null ? code : type);

	    resp.setContentType(accept);

	    if (accept.contains("text/plain")) {
	    	out.println("ERROR " + uri + " " + reason + ": " + message);
	    	if (throwable != null) {
	    		throwable.printStackTrace(out);
	    	}
	    } else {
	    	out.println("<html><body><h1>Error</h1><p>Status code " + reason + "</p><p>" + uri + "</p><p>" + message + "</p>");
	    	if (throwable != null) {
	    		out.println("<pre>");
	    		throwable.printStackTrace(out);
	    		out.println("</pre>");
	    	}
	    	out.println("</body><html>");
	    }
	    out.flush();
	    out.close();
	}

	// TM
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		doGet(req, resp);	
	}

	// TM
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		doGet(req, resp);	
	}
}
