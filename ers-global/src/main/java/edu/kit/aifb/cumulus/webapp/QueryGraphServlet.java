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

/** 
 * 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryGraphServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();

		// force only text/plain for output in this case
		SerializationFormat formatter = Listener.getSerializationFormat("text/plain");
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		int queryLimit = (Integer)ctx.getAttribute(Listener.QUERY_LIMIT);
		resp.setCharacterEncoding("UTF-8");

		String g = req.getParameter("g");
		String l = req.getParameter("limit");
		if( g == null || g.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the graph as 'g' parameter");
			return;
		}
		if( !g.startsWith("<") || !g.endsWith(">") )  {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please a resource as graph name.");
			return;
		}
		// limit param is optional
		Integer limit=Integer.MAX_VALUE;
		if( l!=null && !l.isEmpty() ) { 
			try{
				limit = Integer.parseInt(l);
			} catch( NumberFormatException ex ) { 
				_log.severe("Exception: " + ex.getMessage() );
				sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a number as limit or nothing at all.");
			}
		}
		CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);

		// do not allow querying of system keyspaces, authors or graphs
		if( g.startsWith("system") || g.equals(Listener.GRAPHS_NAMES_KEYSPACE) || g.equals(Listener.GRAPHS_NAMES_KEYSPACE) ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_FORBIDDEN, "It is forbidden to query this keyspace " + g);
			return;
		}
		// check if graph exists 
		if( ! crdf.existsKeyspace(Store.encodeKeyspace(g)) ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "The graph " + g + " passed as input does not exist. No data returned.");
			return;
		}
		int triples = crdf.queryEntireKeyspace(Store.encodeKeyspace(g), out, limit);
		if( triples == 0 ) 
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "The graph " + g + " is empty. No data returned.");
		 else if( triples == -1 ) 
			sendResponse(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "An exception was thrown while querying graph " + g + ". No data returned. Check logs.");
		 else {
			String msg = "OK " + req.getRequestURI() + " " + String.valueOf(HttpServletResponse.SC_OK) + ": " + triples + " quad(s) found. All of them are listed below.";
			resp.getWriter().println(msg);
			out.close();
			resp.getWriter().print(buffer.toString());
		 }
		_log.info("[dataset] QUERY THE WHOLE GRAPH " + (System.currentTimeMillis() - start) + "ms " + triples + "t");
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
