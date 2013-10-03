package edu.kit.aifb.cumulus.webapp;

import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;

import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import me.prettyprint.hector.api.beans.HColumn;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 *
 * @author aharth
 */
@SuppressWarnings("serial")
public class QueryBridgesStatsServlet extends AbstractHttpServlet {
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
                String stat = req.getParameter("stat");
                String time_span = req.getParameter("span");
		// some checks
		if( ip != null && ip.isEmpty() ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Please pass an IP as ip parameter. Default is 'all'");
			return;
                }
                if( stat == null || stat.isEmpty() ) {
                    // use default
                    stat = "NUM";
                }
                String start_time, end_time;
                long current_time = System.currentTimeMillis();
                end_time = String.valueOf(current_time);
                long start_time_l;
                if( time_span == null || time_span.isEmpty() ) {
                    // default: last minute
                    start_time_l = current_time-(60*1000);
                    start_time = String.valueOf(start_time_l);
                }
                else {
                    // possible values: 1 (last min), 60 (last h), 3600 (last day)
                    Integer time_span_int = Integer.valueOf(time_span);
                    // *1000 because current time is measured in miliseconds
                    // *60 as the timespan is given in minutes
                    start_time_l = current_time-(1000*time_span_int*60);
                    start_time = String.valueOf(start_time_l);
                }

                // get all keyspaces
		Node[] query = new Node[3];
		try {
			query[0] = getNode("<"+ip+"_TOTAL_NUM>", "s");
                        query[1] = getNode(null, "p");
			query[2] = getNode(null, "o");
		}
		catch (ParseException ex) {
			_log.severe(ex.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Could not parse query string.");
			return;
		}
		CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);

                List<String> keyspaces = new ArrayList<String>();
                try {
                    Iterator<Node[]> it;
                    it = crdf.query(query, Listener.BRIDGES_KEYSPACE);
                    resp.setContentType(formatter.getContentType());
                    while (it.hasNext()) {
                        Node[] n = it.next();
                        keyspaces.add(n[1].toString());
                    }
                } catch (StoreException ex) {
                        _log.severe(ex.getMessage());
                        resp.sendError(500, ex.getMessage());
                }

                // for each keyspace, get data within interval
                Iterator<String> it = keyspaces.iterator();
                while( it.hasNext() ) {
                    String keyspace = it.next();
                    // get all keyspaces
                    Node[] query_st = new Node[3];
                    try {
                            query_st[0] = getNode("<"+keyspace+"_"+stat+">", "s");
                            query_st[1] = getNode(null, "p");
                            query_st[2] = getNode(null, "o");
                    }
                    catch (ParseException ex) {
                            _log.severe(ex.getMessage());
                            sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                    "Could not parse query string.");
                            break;
                    }
                    Iterator<HColumn<String,String>> it_st;
                    try {
                        it_st = crdf.queryBrigesTimeStats(query_st,
                                Store.encodeKeyspace(Listener.BRIDGES_KEYSPACE + "_" + ip),
                                "\""+start_time+"\"", "\""+end_time+"\"");
                        boolean had_results = false;
                        if( it_st.hasNext() ) {
                            resp.getWriter().print(crdf.decodeKeyspace(keyspace)+":");
                            had_results = true;
                        }
                        while(it_st.hasNext()) {
                            HColumn<String,String> column = it_st.next();
                             resp.getWriter().print( " ("+column.getName()
                                     + "," + column.getValue()+")" );
                        }
                        if( had_results )
                            resp.getWriter().print("\n");
                    } catch (StoreException ex) {
                        Logger.getLogger(QueryBridgesStatsServlet.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                out.close();
                resp.getWriter().print(buffer.toString());
                // header needed to allow local javascript http requests
                resp.addHeader("Access-Control-Allow-Origin", req.getHeader("Origin"));
		_log.info("[dataset] QUERY BRIDGE STATS " + (System.currentTimeMillis() - start) + "ms ");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
