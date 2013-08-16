package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.util.Iterator;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
/** 
 * 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class LastSyncSeqServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		// force only text/plain for output in this case
		SerializationFormat formatter = Listener.getSerializationFormat("text/plain");
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		String resource = "<resource>";
		int queryLimit = (Integer)ctx.getAttribute(Listener.QUERY_LIMIT);
		resp.setCharacterEncoding("UTF-8");

		String a = req.getParameter("g");
		// some checks
        if( a == null || a.isEmpty() ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the graph name as g parameter.");
			return;
        }
		if( a!=null && !a.isEmpty() && (!a.startsWith("<") || !a.endsWith(">")) && (!a.startsWith("\"") || !a.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as graph name.");
			return;
		}
		Node[] query = new Node[3];
		try {
			query[0] = getNode("\""+Store.encodeKeyspace(a)+"\"", "s");
			query[1] = getNode(Listener.SEQ_NUMBER_PROPERTY, "p");
			query[2] = getNode(null, "o");
		}
		catch (ParseException ex) {
			_log.severe(ex.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Could not parse query string.");
			return;
		}

		if (query[0] instanceof Variable && query[1] instanceof Variable && query[2] instanceof Variable) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Query must contain at least one constant.");
			return;
		}
		String graph = a;
		CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
		if( a != null && ! a.isEmpty() ) {
			if( ! crdf.existsKeyspace(Store.encodeKeyspace(a)) ) { 
				sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Graph "+graph+" does not exist.");
                return;
            }
		}

		BufferedWriter bw = new BufferedWriter(out);
		try {
			Iterator<Node[]> it = crdf.query(query, 1, Listener.GRAPHS_NAMES_KEYSPACE);
			if (it.hasNext()) {
                Node[] n = (Node[])it.next();
				resp.setContentType(formatter.getContentType());
    			resp.getWriter().println(n[2]);
	    		out.close();
			}
            else {
		        int r = crdf.addData("\""+Store.encodeKeyspace(a)+"\"", Listener.SEQ_NUMBER_PROPERTY, "\"0\"", 
                            Listener.GRAPHS_NAMES_KEYSPACE, 0);
                resp.getWriter().println(0);
            }
		} catch (StoreException ex) {
			_log.severe(ex.getMessage());
			resp.sendError(500, ex.getMessage());
		}
		_log.info("[dataset] QUERY " + Nodes.toN3(query) + " " + (System.currentTimeMillis() - start) + "ms " );
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2) 
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
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
		String g = req.getParameter("g");  	        // graph = keyspace_name 
        String seq_n = req.getParameter("seq");     // the new sequence number 
        
		// some checks
		if( g == null || seq_n == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass data like 'g=_&seq=_");
			return;
		}
		if( g.isEmpty() || seq_n.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass non empty data 'g=_&seq=_'");
			return;
		}
		if( (!g.startsWith("<") || !g.endsWith(">")) && (!g.startsWith("\"") || !g.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. <resource>) as graph name.");
			return;
		}	
		// escape if the accept header is not text/plain
		String graph = g;
		if ( ! formatter.getContentType().equals("text/plain") ) 
			graph = escapeHtml(g);

		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);

    	Node[] query = new Node[3];
		try {
			query[0] = getNode("\""+Store.encodeKeyspace(g)+"\"", "s");
			query[1] = getNode(Listener.SEQ_NUMBER_PROPERTY, "p");
			query[2] = getNode(null, "o");
		}
		catch (ParseException ex) {
			_log.severe(ex.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Could not parse query string.");
			return;
		}
		if (query[0] instanceof Variable && query[1] instanceof Variable && query[2] instanceof Variable) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Query must contain at least one constant.");
			return;
		}
		if( g != null && ! g.isEmpty() ) {
			if( ! crdf.existsKeyspace(Store.encodeKeyspace(g)) ) { 
				sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Graph "+graph+" does not exist.");
                return;
            }
		}

		BufferedWriter bw = new BufferedWriter(out);
        String old_value ="";
		try {
			Iterator<Node[]> it = crdf.query(query, 1, Listener.GRAPHS_NAMES_KEYSPACE);
			if (it.hasNext()) {
                Node[] n = (Node[])it.next();
    			old_value = n[2].toString();
			}
		} catch (StoreException ex) {
			_log.severe(ex.getMessage());
			resp.sendError(500, ex.getMessage());
		}
		// do the update here 
		if( crdf.updateData("\""+Store.encodeKeyspace(g)+"\"", Listener.SEQ_NUMBER_PROPERTY, "\""+old_value+"\"", "\""+seq_n+"\"", Listener.GRAPHS_NAMES_KEYSPACE, 0) == -2 )
			sendError(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Graph " + graph + " does not exist.");
		else {
			String msg = "Quad (\""+Store.encodeKeyspace(g)+"\","+Listener.SEQ_NUMBER_PROPERTY+","+seq_n+","+Listener.GRAPHS_NAMES_KEYSPACE+") has been added.";
			if ( formatter.getContentType().equals("text/html") )
				msg = escapeHtml(msg);
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, msg);
		}
		_log.info("[dataset] POST " + (System.currentTimeMillis() - start) + "ms ");
	}
	
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("DELETE currently not supported, sorry.");
	}

	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
