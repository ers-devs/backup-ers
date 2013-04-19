package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
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
 * @author aharth
 */
@SuppressWarnings("serial")
public class QueryServlet extends AbstractHttpServlet {
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

		String e = req.getParameter("e");
		String p = req.getParameter("p");
		String v = req.getParameter("v");
		String a = req.getParameter("g");
//		_log.info("QUERYServlet: req " + req.getPathInfo() + " " + req.getQueryString() + " " + e + " " + p + " " + v);
		// some checks
		if( e != null && !e.isEmpty() && !e.startsWith("<") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass a resource (e.g. &lt;resource&gt;) as entity");
			return;
		}	
		if( p!= null && !p.isEmpty() && !p.startsWith("<") && !p.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass either a resource (e.g. &lt;resource&gt;) or a literal (e.g. \"literal\") as property");
			return;
		}
		if( v!=null && !v.isEmpty() && !v.startsWith("<") && !v.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass either a resource (e.g. &lt;resource&gt;) or a literal (e.g. \"literal\") as value");
			return;
		}
		Node[] query = new Node[3];
		try {
			query[0] = getNode(e, "s");
			query[1] = getNode(p, "p");
			query[2] = getNode(v, "o");
		}
		catch (ParseException ex) {
			_log.severe(ex.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "could not parse query string");
			return;
		}

		if (query[0] instanceof Variable && query[1] instanceof Variable && query[2] instanceof Variable) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "query must contain at least one constant");
			return;
		}
		PrintWriter out = resp.getWriter();
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
		int triples = 0;

		// search within given keyspace or all if none is given
		List<String> keyspaces = new ArrayList<String>(); 
		if( a != null && ! a.isEmpty() ) 
			keyspaces.add(Store.encodeKeyspace(a)); 
		else 
			keyspaces = crdf.getAllKeyspaces(); 

		boolean found = false;
		for(Iterator it_k = keyspaces.iterator(); it_k.hasNext(); ) { 
			String k = (String)it_k.next();
			// skip keyspaces that do not use our pre-defined prefix or the authors one
			if( ! k.startsWith(Listener.DEFAULT_ERS_KEYSPACES_PREFIX) || 
			      k.equals(Listener.AUTHOR_KEYSPACE) )
				continue;
			try {
				Iterator<Node[]> it = crdf.query(query, queryLimit, k);
				if (it.hasNext()) {
					resp.setContentType(formatter.getContentType());
					triples = formatter.print(it, out, k);
					found = true;
				}
			} catch (StoreException ex) {
				_log.severe(ex.getMessage());
				resp.sendError(500, ex.getMessage());
			}
		}
		if( !found ) 
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_FOUND, "resource not found");
		_log.info("[dataset] QUERY " + Nodes.toN3(query) + " " + (System.currentTimeMillis() - start) + "ms " + triples + "t");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2) 
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
