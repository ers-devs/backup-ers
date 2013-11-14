package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;

import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 *
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryZookeeperStatsServlet extends AbstractHttpServlet {
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
		resp.setCharacterEncoding("UTF-8");
                // header needed to allow local javascript http requests
                resp.addHeader("Access-Control-Allow-Origin", "*");

                // default no of trials
                int trials_int = 100;
		String trials = req.getParameter("trials");
                String id = req.getParameter("id");
                if( trials == null || trials.isEmpty() ) {
                    trials_int = Integer.parseInt(trials);
                }
                InterProcessReadWriteLock rw_lock;
                Integer TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS = 10;
                int read_lock=0, failed_read_lock=0;
                int write_lock=0, failed_write_lock=0;
                long total_acquire_read_time=0;
                long total_release_read_time=0;
                long total_acquire_write_time=0;
                long total_release_write_time=0;
                long start_time;

                if( id == null || id.isEmpty() ) {
                    id = UUID.randomUUID().toString();
                }

                for( int i=0; i<trials_int; ++i ) {
                    String key_e = "eeeeeeeeeeeeeeeeeeee"+i+"-"+id;
                    rw_lock = new InterProcessReadWriteLock(Listener.curator_client, "/e/"+key_e);
                    start_time = System.currentTimeMillis();
                    // acquire and release a read lock here
                    try {

                        if( ! rw_lock.readLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS,
                                TimeUnit.MILLISECONDS) ) {
                            failed_read_lock++;
                        }
                        else {
                            read_lock++;
                            total_acquire_read_time += System.currentTimeMillis()-start_time;

                            start_time = System.currentTimeMillis();
                            rw_lock.readLock().release();
                            total_release_read_time += System.currentTimeMillis()-start_time;
                        }
                    }
                    catch( Exception ex ) {
                        failed_read_lock++;
                    }

                    // acquire and release a write lock here
                    try {

                        if( ! rw_lock.writeLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS,
                                TimeUnit.MILLISECONDS) ) {
                            failed_write_lock++;
                        }
                        else {
                            write_lock++;
                            total_acquire_write_time += System.currentTimeMillis()-start_time;

                            start_time = System.currentTimeMillis();
                            rw_lock.writeLock().release();
                            total_release_write_time += System.currentTimeMillis()-start_time;
                        }
                    }
                    catch( Exception ex ) {
                        failed_write_lock++;
                    }
                }
                StringBuilder sb = new StringBuilder();
                // read locks
                sb.append("total_read_locks, total_failed_read_locks,")
                        .append("avg_acquire_read_lock_ms, avg_release_read_lock_ms")
                        .append("\n");
                sb.append(read_lock).append(" ").append(failed_read_lock).append(" ");
                sb.append(total_acquire_read_time/read_lock).append(" ");
                sb.append(total_release_read_time/read_lock).append("\n");

                //write locks
                sb.append("total_write_locks, total_failed_write_locks,")
                        .append("avg_acquire_write_lock_ms, avg_release_write_lock_ms")
                        .append("\n");
                sb.append(write_lock).append(" ").append(failed_write_lock).append(" ");
                sb.append(total_acquire_write_time/write_lock).append(" ");
                sb.append(total_release_write_time/write_lock).append("\n");

                resp.getWriter().println(sb.toString());
		resp.getWriter().flush();
		_log.info("[dataset] QUERY ZOOKEEPER " + read_lock + " " + write_lock);
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
