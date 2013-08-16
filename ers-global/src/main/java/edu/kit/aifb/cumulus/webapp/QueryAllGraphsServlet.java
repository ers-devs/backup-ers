package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.List; 
import java.util.ArrayList;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

/** 
 * 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryAllGraphsServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();

		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		int queryLimit = (Integer)ctx.getAttribute(Listener.QUERY_LIMIT);
		resp.setCharacterEncoding("UTF-8");

		//NOTE: this refers to result limit per graph, NOT total limit
		String l = req.getParameter("limit"); 
		if( l == null || l.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the limit as 'limit' parameter");
			return;
		}
		int limit=0; 
		try { 
			limit = Integer.parseInt(l);
		} catch( NumberFormatException ex ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the limit as integer parameter");
			return;
		}
		CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);

		int quads = crdf.queryAllKeyspaces(limit, out);
		if( quads == 0 ) 
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "The data store empty. No data returned.");
		else { 
			String msg = "OK " + req.getRequestURI() + " " + String.valueOf(HttpServletResponse.SC_OK) + ": " + quads + " quad(s) found. All of them are listed below.";
			resp.getWriter().println(msg);
			out.close();
			resp.getWriter().print(buffer.toString());
		 }
		_log.info("[dataset] QUERY THE WHOLE DATASET "+ (System.currentTimeMillis() - start) + "ms " + quads + " quads");
		return;
	}

	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("POST currently not supported, sorry.");
	}
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("DELETE currently not supported, sorry.");
	}
}
