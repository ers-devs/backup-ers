package edu.kit.aifb.cumulus.webapp;

import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.StoreException;
import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;

import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.util.Iterator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 *
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryBridgesIPServlet extends AbstractHttpServlet {
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

		String ip = req.getParameter("ip");
		// some checks
		if( ip != null && ip.isEmpty() ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, 
                                "Please pass an IP as ip parameter. Default is 'all'");
			return;
                }
                if( ip == null || ip.isEmpty() ) {
                    ip = "all";
                }
		Node[] query = new Node[3];
		try {
			query[0] = getNode("<bridges>", "s");
                        if( ip != null && !ip.isEmpty() && !ip.equals("all") )
                            query[1] = getNode("\""+ip+"\"", "p");
                        else
                            query[1] = getNode(null, "p");
                        
			query[2] = getNode(null, "o");
		}
		catch (ParseException ex) {
			_log.severe(ex.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Could not parse query string.");
			return;
		}
		if (query[0] instanceof Variable && query[1] instanceof Variable && query[2] instanceof Variable) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Query must contain at least one constant.");
			return;
		}
		CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
		int triples = 0;

		int total_triples=0;
		BufferedWriter bw = new BufferedWriter(out);
                try {
                    Iterator<Node[]> it;
                    it = crdf.query(query, Listener.BRIDGES_KEYSPACE);
                    resp.setContentType(formatter.getContentType());
                    while (it.hasNext()) {
                        Node[] curr = it.next();
                        String output = curr[1].toString() + " " + curr[2].toString();

                        // get total number of entities synched for all keyspaces
                        String ip_bridge = curr[1].toString();
                        Node[] query_total_num = new Node[3];
                        try {
                            query_total_num[0] = getNode("<"+ip_bridge+"_TOTAL_NUM>", "s");
                            query_total_num[1] = getNode(null, "p");
                            query_total_num[2] = getNode(null, "o");
                        }
                        catch (ParseException ex) {
                            _log.severe(ex.getMessage());
                            sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                    "Could not parse query string.");
                        }
                        Iterator<Node[]> it_total_num = crdf.query(query_total_num,
                                Listener.BRIDGES_KEYSPACE);
                        int no_diff_keyspaces=0;
                        int total_ent_synched=0;

                        while( it_total_num.hasNext() ) {
                            no_diff_keyspaces++;
                            Node[] no_bytes = it_total_num.next();
                            total_ent_synched+=Integer.valueOf(no_bytes[2].toString());
                        }
                        output += " " + no_diff_keyspaces + " " + total_ent_synched;

                        // get total bytes synched for all keyspaces
                        Node[] query_total_bytes = new Node[3];
                        try {
                            query_total_bytes[0] = getNode("<"+ip_bridge+"_TOTAL_BYTES>", "s");
                            query_total_bytes[1] = getNode(null, "p");
                            query_total_bytes[2] = getNode(null, "o");
                        }
                        catch (ParseException ex) {
                            _log.severe(ex.getMessage());
                            sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                    "Could not parse query string.");
                        }
                        Iterator<Node[]> it_total_bytes = crdf.query(query_total_bytes,
                                Listener.BRIDGES_KEYSPACE);
                        int total_bytes_synched=0;
                        while( it_total_bytes.hasNext() ) {
                            Node[] no_bytes = it_total_bytes.next();
                            total_bytes_synched+=Integer.valueOf(no_bytes[2].toString());
                        }
                        output += " " + total_bytes_synched;

                        resp.getWriter().write(output+"\n");
                        total_triples++;
                    }
                } catch (StoreException ex) {
                        _log.severe(ex.getMessage());
                        resp.sendError(500, ex.getMessage());
                }
                String msg = "OK " + req.getRequestURI() + " " + 
                        String.valueOf(HttpServletResponse.SC_OK) + ": "
                        + total_triples + " quad(s) found. All of them are listed below.";
                resp.getWriter().println(msg);
                out.close();
                resp.getWriter().print(buffer.toString());
                // header needed to allow local javascript http requests 
                resp.addHeader("Access-Control-Allow-Origin", req.getHeader("Origin"));
		_log.info("[dataset] QUERY " + Nodes.toN3(query) + " " + (System.currentTimeMillis() - start) + "ms " + triples + "t");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
