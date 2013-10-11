package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
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
 * @author aharth
 */
@SuppressWarnings("serial")
public class QueryServlet extends AbstractHttpServlet {
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
		String resource = "<resource>";
		int queryLimit = (Integer)ctx.getAttribute(Listener.QUERY_LIMIT);
		resp.setCharacterEncoding("UTF-8");

		String e = req.getParameter("e");
		String p = req.getParameter("p");
		String v = req.getParameter("v");
		String a = req.getParameter("g");
		String l = req.getParameter("limit");
                // params regarding versioning
                String URN = req.getParameter("urn");
                String version_ID = req.getParameter("verID");
		// some checks
		if( e != null && !e.isEmpty() && (!e.startsWith("<") || !e.endsWith(">")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as entity");
			return;
		}	
		if( p!= null && !p.isEmpty() && (!p.startsWith("<") || !p.endsWith(">")) && (!p.startsWith("\"") || !p.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass either a resource (e.g. "+resource+") or a literal (e.g. \"literal\") as property");
			return;
		}
		if( v!=null && !v.isEmpty() && (!v.startsWith("<") || !v.endsWith(">")) && (!v.startsWith("\"") || !v.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass either a resource (e.g. "+resource+") or a literal (e.g. \"literal\") as value");
			return;
		}
		if( a!=null && !a.isEmpty() && (!a.startsWith("<") || !a.endsWith(">")) && (!a.startsWith("\"") || !a.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as graph name.");
			return;
		}
		Integer limit=Integer.MAX_VALUE;
		if( l!=null && !l.isEmpty() ) { 
			try{
				limit = Integer.parseInt(l);
			} catch( NumberFormatException ex ) { 
				_log.severe("Exception: " + ex.getMessage() );
				sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                        "Please pass a number as limit or nothing at all.");
			}
		}
                if( version_ID != null && version_ID.isEmpty() )
                    version_ID = null;
                if( URN != null && URN.isEmpty() )
                    URN = null;
                if( version_ID != null && version_ID.equals("last") && URN == null ) {
                    sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                            "In case of using 'last' version, please give URN as well. ID is defined per URN.");
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
		int triples = 0;

		// search within given keyspace or all if none is given
		List<String> keyspaces = new ArrayList<String>(); 
		if( a != null && ! a.isEmpty() ) {
			if( ! crdf.existsKeyspace(Store.encodeKeyspace(a)) ) 
				sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Graph "+graph+" does not exist.");
			keyspaces.add(Store.encodeKeyspace(a)); 
		}
		else 
			keyspaces = crdf.getAllKeyspaces(); 

                int cutSuffixPos = 0;
                // (S,?,?) or (S,?,V) 
                if( (p == null || p.isEmpty()) && ( v!= null && !v.isEmpty()) )
                    cutSuffixPos = 2;

		boolean found = false;
		int total_triples=0;
		BufferedWriter bw = new BufferedWriter(out);
		for(Iterator it_k = keyspaces.iterator(); it_k.hasNext(); ) { 
			String k = (String)it_k.next();
			// skip keyspaces that do not use our pre-defined prefix or that are in the blacklist
			if( ! k.startsWith(Listener.DEFAULT_ERS_KEYSPACES_PREFIX) || 
			      Listener.BLACKLIST_KEYSPACES.contains(k) )
				continue;
			try {
                            Iterator<Node[]> it;
                            // is verioning enabled for this keyspace?
                            if( crdf.keyspaceEnabledVersioning(crdf.decodeKeyspace(k)) ) {
                                if( e == null || e.isEmpty() ) {
                                    sendResponse(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                            "A versioned graph cannot be querying without passing the entity key!.");
                                    return;
                                }
                                //_log.info("Query the versioning keyspace " + k );
                    		it = crdf.queryVersioning(query, k, version_ID, URN);
            //formatter = new NTriplesVersioningFormat(query);
                            }
                            else
                                it = crdf.query(query, k);
                            
                            if (it.hasNext()) {
                                    resp.setContentType(formatter.getContentType());
                                    triples = formatter.print(it, out, 
                                            crdf.decodeKeyspace(k), cutSuffixPos);
                                    found = true;
                                    total_triples += triples;
                                    limit -= triples;
                                    if( limit == 0 )
                                            break;
                            }
                        } catch (StoreException ex) {
				_log.severe(ex.getMessage());
				resp.sendError(500, ex.getMessage());
			}
		}
		if( !found ) 
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "None quads found.");
		else {
			String msg = "OK " + req.getRequestURI() + " " + String.valueOf(HttpServletResponse.SC_OK) + ": " + total_triples + " quad(s) found. All of them are listed below.";
			resp.getWriter().println(msg);
			out.close();
			resp.getWriter().print(buffer.toString());
		}
			
		_log.info("[dataset] QUERY " + Nodes.toN3(query) + " " + (System.currentTimeMillis() - start) + "ms " + triples + "t");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2) 
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
