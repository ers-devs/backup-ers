package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;
/** 
 * 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class ExistGraphServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		if (req.getCharacterEncoding() == null)
			req.setCharacterEncoding("UTF-8");
		resp.setCharacterEncoding("UTF-8");

		String uri = req.getRequestURI();
		if (uri == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		String g = req.getParameter("g");  	//author = keyspace_name 
		if( g == null || g.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please the name of graph as 'g' parameter'");
			return;
		}	
		if( g.startsWith("system") ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "queries about 'system*' keyspaces is forbidden");
			return;
		}
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		// escape if the accept header is not text/plain
		String graph = g;
		if ( formatter.getContentType().equals("text/html") ) 
			graph = escapeHtml(g);

		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// test if graph exists
		boolean r = crdf.existsKeyspace(Store.encodeKeyspace(g));
		if ( r )
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "TRUE - graph " + graph + " exists.");
		else 
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "FALSE - graph " + graph + " does not exist.");
		_log.info("[dataset] GET exist keyspace " + (System.currentTimeMillis() - start) + "ms ");
	}
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("POST currently not supported, sorry.");
	}
	
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("GET currently not supported, sorry.");
	}

	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
