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
public class AuthorServlet extends AbstractHttpServlet {
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
		// get all existing authors here 
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
		String a_id = req.getParameter("a_id"); //author id as entity
		String a_p = req.getParameter("a_p");	//property
		String a_v = req.getParameter("a_v");	//value
		// some checks
		if( a_id == null || a_p == null || a_v == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass data like 'a_id=_&a_p=_&a_v=_'");
			return;
		}
		if( a_id.isEmpty() || a_p.isEmpty() || a_v.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass non empty data 'a_id=_&a_p=_&a_v=_'");
			return;
		}
		if( !a_id.startsWith("<") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass a resource (e.g. &lt;resource&gt;) as entity / author id");
			return;
		}	
		if( !a_p.startsWith("<") && !a_p.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass either a resource (e.g. &lt;resource&gt;) or a literal (e.g. \"literal\") as property");
			return;
		}
		if( !a_v.startsWith("<") && !a_v.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass either a resource (e.g. &lt;resource&gt;) or a literal (e.g. \"literal\") as value");
			return;
		}	
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// create the associated keyspace with this author, if it does not exist  
		int r = crdf.createKeyspace(a_id.replace("<","").replace(">",""));
		String msg = "";
		if( r == 2 ) 
			msg = "Author " + a_id + " cannot be created. Do not use 'system' as prefix.";
		else {
			// now insert the triple into Authors keyspace 
			int r2 = crdf.addData(a_id, a_p, a_v, Listener.AUTHOR_KEYSPACE);
			if( r2 != -1 ) { 
				if (r == 1) 
					msg = "Author " + a_id + " already exists. New data has been added.";
				else if (r == 0) 
					msg = "Author " + a_id + " has been created. Data added.";
			}
			else 
				msg = "Error on adding the triple !";
		}
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		out.print(msg);
		_log.info("[dataset] POST create/edit author  " + a_id + " " + (System.currentTimeMillis() - start) + "ms ");
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
		String a_id = req.getParameter("a_id");
		if( a_id == null || a_id.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass the author id as 'a_id' parameter");
			return;
		}
		if( !a_id.startsWith("<") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass either a resource (e.g. &lt;resource&gt;) as property");
			return;
		}
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		// do the deletion of entities associated with this author  
		crdf.dropKeyspace(a_id.replace("<","").replace(">",""));
		// delete also the record from AUTHORS keyspace ?! for the moment, NO

		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		out.println("Entities of author " + a_id + " have been deleted.");

		_log.info("[dataset] DELETE keyspace(graph) " + (System.currentTimeMillis() - start) + "ms ");
	}

	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
