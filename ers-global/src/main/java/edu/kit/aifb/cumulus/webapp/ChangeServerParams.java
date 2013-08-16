package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/** 
 * 
 * @author tmacicas
 * Use this servlet to change server side params like consistency and locking granularity.
 */
@SuppressWarnings("serial")
public class ChangeServerParams extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

        private String[] consistency_levels = {"one", "two", "three", "any", "quorum", "all"};
        private String[] granularity_levels = {"e", "ep", "epv" };
        private String[] trans_support_modes = {"zookeeper", "default" };

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("GET currently not supported, sorry.");
	}
	
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
                resp.setContentType(formatter.getContentType());

		String read_cons = req.getParameter("read_cons"); 	//read consistency
		String write_cons = req.getParameter("write_cons");	//write consistency
		String trans_lock_gran = req.getParameter("trans_lock_gran");	//transactiona locking granularity
                String replication_factor = req.getParameter("repl_factor");    //replication factor
                String trans_support = req.getParameter("trans_support");    //replication factor
		// some checks
		if( read_cons == null && write_cons == null && trans_lock_gran == null 
                        && replication_factor == null && trans_support == null ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass data like " +
                                "'read_cons=_&write_cons=_&trans_lock_gran=_&repl_factor=_&trans_support=_'");
                        _log.info("Please pass data like 'read_cons=_&write_cons=_&trans_lock_gran=_&repl_factor=_&trans_support=_'");
			return;
		}
                boolean match_rc = false;
                if( read_cons != null && !read_cons.isEmpty() ) {
                    // check if a correct consistency level
                    for( int i=0; i<consistency_levels.length; i++) {
                        if( read_cons.matches(consistency_levels[i]) ) {
                            match_rc = true;
                            break;
                        }
                    }
                    if( ! match_rc ) {
                        sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Not an " +
                                "allowed read consistency level passed!");
                        _log.info("Not an allowed read consistency level passed!");
                        return;
                    }
                }
                boolean match_wc = false;
                if( write_cons != null && !write_cons.isEmpty() ) {
                    // check if a correct consistency level
                    for( int i=0; i<consistency_levels.length; i++) {
                        if( write_cons.matches(consistency_levels[i]) ) {
                            match_wc = true;
                            break;
                        }
                    }
                    if( ! match_wc ) {
                        sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Not an " +
                                "allowed write consistency level passed!");
                        _log.info("Not an allowed write consistency level passed!");
                        return;
                    }
                }
                if( read_cons !=null && !read_cons.isEmpty() &&
                    write_cons != null && !write_cons.isEmpty() ) {
                    Listener.changeConsistency(read_cons, write_cons);
                    sendResponse(ctx, req, resp, HttpServletResponse.SC_OK,
                            "New read consistency level: " + read_cons + "\n" +
                            "New write consistnecy level: " + write_cons);
                    _log.info("New read consistency level: " + read_cons + "\n" +
                            "New write consistnecy level: " + write_cons);

                }
                if( (read_cons != null && write_cons == null) ||
                    (read_cons == null && write_cons !=null)) {
                    sendResponse(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                            "Both read and wirte consistency must be passed! " +
                            "Use 'read_cons=_&write_cons=_' params!");
                    _log.info("Both read and wirte consistency must be passed! " +
                            "Use 'read_cons=_&write_cons=_' params!");
                }

                if( trans_lock_gran != null && !trans_lock_gran.isEmpty() ) {
                     boolean match = false;
                    // check if a correct consistency level
                    for( int i=0; i<granularity_levels.length; i++) {
                        if( trans_lock_gran.matches(granularity_levels[i]) ) {
                            match = true;
                            break;
                        }
                    }
                    if( ! match ) {
                        sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, 
                                "Not an allowed transaction locking granularity level passed!");
                        _log.info("Not an allowed transaction locking granularity level passed!");
                        return;
                    }
                     Listener.changeLockingGranulartiy(trans_lock_gran);
                     sendResponse(ctx, req, resp, HttpServletResponse.SC_OK,
                            "New locking granularity level: " + trans_lock_gran);
                     _log.info("New locking granularity level: " + trans_lock_gran);
                }

                if( replication_factor != null && !replication_factor.isEmpty() ) {
                    int factor = Integer.valueOf(replication_factor);
                    if( factor < 0 ) {
                        sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Not an allowed replication factor passed");
                        return;
                    }
                    Listener.changeReplicationFactor(factor);
                    sendResponse(ctx, req, resp, HttpServletResponse.SC_OK,
                            "New replication factor: " + factor);
                    _log.info("New replication factor: " + factor);
                }

                if( trans_support != null && !trans_support.isEmpty() ) {
                     boolean match = false;
                    // check if a correct consistency level
                    for( int i=0; i<trans_support_modes.length; i++) {
                        if( trans_support.matches(trans_support_modes[i]) ) {
                            match = true;
                            break;
                        }
                    }
                    if( ! match ) {
                        sendResponse(ctx, req, resp, HttpServletResponse.SC_CONFLICT,
                                "Not an allowed transaction support mode passed!");
                        _log.info("Not an allowed transaction support mode passed!");
                        return;
                    }
                     Listener.changeTransactionalSupport(trans_support);
                     sendResponse(ctx, req, resp, HttpServletResponse.SC_OK,
                            "New transactional support mode: " + trans_support);
                     _log.info("New transactional support mode: " + trans_support);
                }
		_log.info("[dataset] SETUP sucessful");
	}
	
        @Override
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("DELETE currently not supported, sorry.");
	}

        @Override
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
