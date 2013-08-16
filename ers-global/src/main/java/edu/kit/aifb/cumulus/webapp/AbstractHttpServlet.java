package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/** 
 * 
 * @author aharth
 */
@SuppressWarnings("serial")
public abstract class AbstractHttpServlet extends HttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	protected void sendError(ServletContext ctx, HttpServletRequest req, HttpServletResponse resp, int sc, String msg) {
		resp.setStatus(sc);
		req.setAttribute("javax.servlet.error.status_code",sc);
		req.setAttribute("javax.servlet.error.message", msg);
		try {
			ctx.getNamedDispatcher("error").include(req, resp);
		}
		catch (ServletException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void sendResponse(ServletContext ctx, HttpServletRequest req, HttpServletResponse resp, int sc, String msg) {
		resp.setStatus(sc);
		req.setAttribute("javax.servlet.resp.status_code",sc);
		req.setAttribute("javax.servlet.resp.message", msg);
		try {
			ctx.getNamedDispatcher("response").include(req, resp);
		}
		catch (ServletException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
