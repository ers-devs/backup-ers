package edu.kit.aifb.cumulus.webapp;

import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.Store;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 *
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryVersionsStatsServlet extends AbstractHttpServlet {
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

		String graph = req.getParameter("graph");
		// some checks
		if( graph == null || graph.isEmpty() ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Please pass the graph as parameter. ");
			return;
                }
                String encoded_graph = Store.encodeKeyspace(graph);
                int total_entities=0;
                int total_versions=0;
                int total_aborts=0;
                int total_conflicts=0;
                List<Long> get_lCID_time = new ArrayList<Long>();
                CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
                AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
                if( ! crdf.existsKeyspace(encoded_graph) ) {
                    sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Please pass an existing graph as parameter. ");
                    return;
                }
                HashSet<String> different_versions = new HashSet<String>();
                HashSet<String> aborted = new HashSet<String>();
                // first of all get all entities for that graph, then the versions tree
                List<String> keys = crdf.queryAllRowKeys(encoded_graph, Integer.MAX_VALUE);
                Iterator<String> it = keys.iterator();
                // for each key of this graph, get the versioning tree
                while( it.hasNext() ) {
                    String key = it.next();
                    // there are key-VER and key-URN, thus skip one of them 
                    if( key.endsWith("URN>") )
                        continue;

                    ++total_entities;
                    // remove the brackets and the VER suffix
                    key = key.substring(1, key.lastIndexOf("-"));

                    Hashtable<String, List<String>> version_history = crdf.getVersionHistory(
                            encoded_graph, key);
                    // get here the total aborts and total conflicts
                    Iterator<String> it_ver = version_history.keySet().iterator();
                    while( it_ver.hasNext() ) {
                        String prevCID = it_ver.next();
                        List<String> lastCID_children = version_history.get(prevCID);
                        different_versions.addAll(lastCID_children);
                        // aborts?
                        if( prevCID.equals("X") ) {
                            aborted.addAll(lastCID_children);
                            continue;
                        }
                        if( !prevCID.equals("-") )
                            different_versions.add(prevCID);
                        // conflicts?
                        if( lastCID_children.size() > 1 ) {
                            // e.g.: if 2 children, then 1 conflict; if 3 children, 2 conflicts
                            total_conflicts += lastCID_children.size()-1;
                        }
                    }

                    // try to get the lastCommitID for this entity and measure the time
                    long start_time = System.currentTimeMillis();
                    crdf.lastCommitTxID(encoded_graph, key);
                    get_lCID_time.add(System.currentTimeMillis() - start_time);
                }
                // get the average and max of getting last commit ID
                long max_get_lCID_time=0;
                long total_get_lCID_time=0;
                for(Iterator<Long> it_time=get_lCID_time.iterator(); it_time.hasNext(); ) {
                    long t = it_time.next();
                    if( max_get_lCID_time < t )
                        max_get_lCID_time = t;
                    total_get_lCID_time += t;
                }
                total_versions = different_versions.size();
                total_aborts = aborted.size();
                String msg = "OK " + req.getRequestURI() + " " +
                        String.valueOf(HttpServletResponse.SC_OK) + "\n";
                msg += " total no entities, total no versions, total no of conflits, " +
                        "total no of aborted tx, avg get lastCID time, max get lastCID time \n";
                msg+= total_entities+", "+total_versions+", "+total_conflicts+", " +
                        total_aborts + ", " + (total_get_lCID_time/get_lCID_time.size()) +
                        "ms, " + max_get_lCID_time +"ms";
                resp.getWriter().println(msg);
                out.close();
                resp.getWriter().print(buffer.toString());
		_log.info("[dataset] QUERY versions stats " + (System.currentTimeMillis() - start)
                        + "ms " + total_entities+ "t"); 
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
