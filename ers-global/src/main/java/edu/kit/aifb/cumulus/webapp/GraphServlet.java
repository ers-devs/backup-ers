package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

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

		// get all existing graphs here 
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
		out.close();
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
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		// escape if the accept header is html
		String resource = "<resource>";
		if ( formatter.getContentType().equals("text/html") )
			resource = escapeHtml(resource); 

		String a_id = req.getParameter("g_id"); //graph id as entity
		String a_p = req.getParameter("g_p");	//property
		String a_v = req.getParameter("g_v");	//value
                String ver = req.getParameter("ver");	//value
		// some checks
		if( a_id == null || a_p == null || a_v == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass data like 'a_id=_&a_p=_&a_v=_'");
			return;
		}
		if( a_id.isEmpty() || a_p.isEmpty() || a_v.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass non empty data 'a_id=_&a_p=_&a_v=_'");
			return;
		}
		if( !a_id.startsWith("<") || !a_id.endsWith(">") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as entity / graph id");
			return;
		}	
		if( (!a_p.startsWith("<") || !a_p.endsWith(">")) && (!a_p.startsWith("\"") || !a_p.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass either a resource (e.g."+resource+") or a literal (e.g. \"literal\") as property");
			return;
		}
		if( (!a_v.startsWith("<") || !a_p.endsWith(">")) && (!a_v.startsWith("\"") || !a_v.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass either a resource (e.g. "+resource+") or a literal (e.g. \"literal\") as value");
			return;
		}
                boolean enable_versioning = false;
                if( ver != null ) {
                    enable_versioning = true;
		}
		// escape if the accept header is html
		String graph = a_id;
		if ( formatter.getContentType().equals("text/html") )
			graph = escapeHtml(a_id);

		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// create the associated keyspace with this graph, if it does not exist  
		int r = crdf.createKeyspace(a_id, enable_versioning);

		String msg = "";
		if( r == 2 ) 
			sendError(ctx, req, resp, HttpServletResponse.SC_FORBIDDEN, "Graph " + graph + " cannot be created. Do not use 'system' as prefix.");
                else {
			// now insert the triple into Graphs keyspace 
			int r2 = crdf.addData("\""+Store.encodeKeyspace(a_id)+"\"", a_p, a_v, Listener.GRAPHS_NAMES_KEYSPACE, 0);
			if( r2 != -1 ) { 
				if (r == 1) 
					sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "Graph " + graph + " already exists. New data has been added.");
				else if (r == 0) 
					sendResponse(ctx, req, resp, HttpServletResponse.SC_CREATED, "Graph " + graph + " has been created. Data added.");
				else if(r == 3) 	
					sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "ERS exception on creating a keyspace!");
			}
			else 
				sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error on adding the triple!");
		}
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		out.print(msg);
		_log.info("[dataset] POST create/edit graph  " + a_id + " " + (System.currentTimeMillis() - start) + "ms ");
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
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "No known mime type in Accept header");
			return;
		}
		// escape if the accept header is html
		String resource = "<resource>";
		if ( formatter.getContentType().equals("text/html") )
			resource = escapeHtml(resource); 

                String truncate = req.getParameter("truncate");
		String a_id = req.getParameter("g");
		String f = req.getParameter("f");
		if( a_id == null || a_id.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the graph id as 'g' parameter.");
			return;
		}
		if( !a_id.startsWith("<") || !a_id.endsWith(">") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as graph.");
			return;
		}
		// escape if the accept header is not text/plain
		String graph = a_id;
		if ( ! formatter.getContentType().equals("text/plain") ) 
			graph = escapeHtml(a_id);

		boolean force = (f != null && f.equals("y")) ? true : false;
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);

		// do the deletion of entities associated with this graph  
		String encoded_keyspace = Store.encodeKeyspace(a_id);
                if( a_id.replace("<","").replace(">","").equals(Listener.GRAPHS_VERSIONS_KEYSPACE) )
                    encoded_keyspace = a_id.replace("<","").replace(">","");
                int r;
                if( truncate != null )
                    r = crdf.truncateKeyspace(encoded_keyspace);
                else
                    r = crdf.dropKeyspace(encoded_keyspace, force);

		switch(r) { 
			case 0:
                                int r2 = -1;
                                if( truncate == null ) {
                                    //delete from ERS_graphs
                                    r2 = crdf.deleteByRowKey("\""+encoded_keyspace+"\"", Listener.GRAPHS_NAMES_KEYSPACE, 0);
                                }
				sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "The entire of graph " + graph + " has been deleted/truncated. DEBUG: " + r2);
				break;
			case 1: 	
				sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "The graph " + graph + " does not exist. Nothing to delete/truncate.");
				break;
			case 2: 
				sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Deleting the graph has raised exceptions. Check Tomcat log.");
				break;
			case 3:	
				// since we are using the hashed graph name, this does not happen
				sendError(ctx, req, resp, HttpServletResponse.SC_FORBIDDEN, "Cannot delete/truncate any graph (keyspace) whose name has the prefix 'system'.");
				break;
			case 4:
				sendError(ctx, req, resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "Cannot delete/truncate a not-empty graph.");
				break;
			default:	
				// ideally, this may never happen :)
				sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "UNKNOWN exit code of deleting/truncating the graph method.");
				break;
		}	
		_log.info("[dataset] DELETE/TRUNCATE keyspace("+graph+") " + (System.currentTimeMillis() - start) + "ms ");
	}

	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
