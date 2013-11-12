package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

/** 
 * 
 * @author aharth
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class CreateServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("GET currently not supported, sorry.");
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

		String e = req.getParameter("e"); 	//entity
		String p = req.getParameter("p");	//property
		String v = req.getParameter("v");	//value
		String a = req.getParameter("g");  	//author = keyspace_name
                String urn = req.getParameter("urn");   //used in case of versioning
		// some checks
		if( e == null || p == null || v == null || a == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass data like 'e=_&p=_&v=_&g=_'");
			return;
		}
		if( e.isEmpty() || p.isEmpty() || v.isEmpty() || a.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass non empty data 'e=_&p=_&v=_&g=_'");
			return;
		}
		if( !e.startsWith("<") || !e.endsWith(">") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as entity");
			return;
		}	
		if( (!p.startsWith("<") || !p.endsWith(">")) && (!p.startsWith("\"") || !p.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass either a resource (e.g. "+resource+") or a literal (e.g. \"literal\") as property");
			return;
		}
		if( (!v.startsWith("<") || !v.endsWith(">")) && (!v.startsWith("\"") || !v.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass either a resource (e.g."+resource+") or a literal (e.g. \"literal\") as value");
			return;
		}	
		if( (!a.startsWith("<") || !a.endsWith(">")) && (!a.startsWith("\"") || !a.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as graph name.");
			return;
		}	
		// escape if the accept header is not text/plain
		String graph = a;
		if ( ! formatter.getContentType().equals("text/plain") ) 
			graph = escapeHtml(a);

		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);

                // check if for given graph versioning is enabled or not
                boolean enabled = crdf.keyspaceEnabledVersioning(a);
                int ret = 0;
                // do the insert here based on versioning flag
                if( enabled ) {
                    if( urn == null || urn.isEmpty() ) {
                        sendError(ctx, req, resp, HttpServletResponse.SC_CONFLICT, 
                                "Graph " + graph + " has versioning enabled. Please " +
                                "pass URN as parameter.");
                        _log.info("[dataset] POST Graph " + graph + " has versioning enabled. Please " +
                                "pass URN as parameter.");
                        return;
                    }
                    ret = crdf.addDataVersioning(e,p,v,Store.encodeKeyspace(a), 
                            0, urn);
                }
                else
                    ret = crdf.addData(e,p,v,Store.encodeKeyspace(a), 0);
                
		if( ret  == -2 ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Graph " + graph + " does not exist.");
                        _log.info("[dataset] POST Graph " + graph + " does not exist.");
                }
		else {
			String msg = "Quad ("+e+","+p+","+v+","+a+") has been added.";
			if ( formatter.getContentType().equals("text/html") )
				msg = escapeHtml(msg);
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, msg);
                        //_log.info("[dataset] POST Graph " + msg);
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
