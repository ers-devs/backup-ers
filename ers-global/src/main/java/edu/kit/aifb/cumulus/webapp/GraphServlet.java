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

/** 
 * 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class GraphServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		if (req.getCharacterEncoding() == null)
			req.setCharacterEncoding("UTF-8");
		resp.setCharacterEncoding("UTF-8");

		// since browsers do not emit DELETE HTTP request, do this trick here
		if (req.getParameter("delete") != null ) {
			doDelete(req,resp);
			return;
		}

		String uri = req.getRequestURI();
		if (uri == null) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// get all existing keyspaces here 
		List<String> keyspaces = ((AbstractCassandraRdfHector)crdf).getAllKeyspaces();
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		if( keyspaces.size() == 0 )  
			out.print("There are none existing graphs");
		else { 
			out.println("All already created graphs listed below:");
			for(Iterator<String> it = keyspaces.iterator(); it.hasNext(); ) { 
				out.println(it.next());
			}
		}
		_log.info("[dataset] GET all graphs (keyspaces) " + (System.currentTimeMillis() - start) + "ms ");
	}
	
	// use POST for creating + updates if it already exists
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
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
		String g = req.getParameter("g");  	//graph_name = keyspace_name 
		// some checks
		if( g == null || g.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass the graph name as 'g' parameter'");
			return;
		}
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// do the insert here 
		int r = ((AbstractCassandraRdfHector)crdf).createKeyspace(g);
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		String msg = "";
		if( r == 0 ) 
			msg = "Graph " + g + " has been created.";
		else if (r == 1)
			msg = "Graph " + g + " already exists.";
		else if (r == 2) 
			msg = "Graph " + g + " cannot be created. Do not use 'system' as prefix.";
		out.print(msg);
		_log.info("[dataset] POST create graph (keyspace) " + g + " " + (System.currentTimeMillis() - start) + "ms ");
	}
	
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
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
		String g = req.getParameter("g");
		if( g == null || g.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass the graph name as 'g' parameter");
			return;
		}
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// do the deletion here 
		int r = crdf.dropKeyspace(g);

		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		if( r == 0 ) 
			out.print("All data deleted");	
		else if ( r == 1 ) 
			out.print("Graph not found. Nothing was deleted.");
		else if ( r == 3 ) 
			out.print("Cannot delete a graph name whose name starts with 'system'.");
		_log.info("[dataset] DELETE keyspace(graph) " + (System.currentTimeMillis() - start) + "ms ");
	}

	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
