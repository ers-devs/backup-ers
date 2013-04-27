package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.lang.StringBuffer;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

/** 
 * 
 * @author aharth
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class UpdateServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
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
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "No known mime type in Accept header");
			return;
		}
		// escape if the accept header is not text/plain
		String resource = "<resource>";
		if ( formatter.getContentType().equals("text/html") )
			resource = escapeHtml(resource); 

		String e = req.getParameter("e"); 
		String p = req.getParameter("p");
		String a = req.getParameter("g");
		// NOTE: we require both v_old and v_new as we allow multiple values for same p => by sending the v_old we can identify with record is asked to be updated
		String v_old = req.getParameter("v_old");
		String v_new = req.getParameter("v_new");
		// some checks
		if( e == null || p == null || v_old == null || v_new == null || a == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass data like 'e=_&p=_&v_old=_&v_new=_&g=_'");
			return;
		}
		if( e.isEmpty() || p.isEmpty() || v_old.isEmpty() || v_new.isEmpty() || a.isEmpty()  ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass non empty data 'e=_&p=_&v_old=_&v_new=_&g=_'");
			return;
		}
		if( !e.startsWith("<") || !e.endsWith(">") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource (e.g. "+resource+") as entity");
			return;
		}
		if( (!p.startsWith("<") || !p.endsWith(">")) && (!p.startsWith("\"") || !p.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please either resources (e.g. "+resource+") or literal (e.g. \"literal\") as property");
			return;
		}
		if( (!v_old.startsWith("<") || !v_old.endsWith(">")) && (!v_old.startsWith("\"") || !v_old.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please either resources (e.g. "+resource+") or literal (e.g. \"literal\") as old value");
			return;
		}	
		if( (!v_new.startsWith("<") || !v_new.endsWith(">")) && (!v_new.startsWith("\"") || !v_new.endsWith("\"")) ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please either resources (e.g. "+resource+") or literal (e.g. \"literal\") as new value");
			return;
		}
		if( !a.startsWith("<") || !a.endsWith(">") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please either resources (e.g. "+resource+") as graph");
			return;
		}
		// escape if the accept header is not text/plain
		String graph = a;
		if ( formatter.getContentType().equals("text/html") ) 
			graph = escapeHtml(a);

		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		// do an update here 
		if( crdf.updateData(e,p,v_old,v_new,Store.encodeKeyspace(a)) == -2 )
			sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, "Graph " + a + " does not exists.");
		else {
			StringBuffer buf = new StringBuffer(); 
			buf.append("Quad ("+e+","+p+","+v_old+","+a+") has been removed. ");
			buf.append("Quad ("+e+","+p+","+v_new+","+a+") has been added.");
			if( formatter.getContentType().equals("text/html") ) 
				sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, escapeHtml(buf.toString()));
			else
				sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, buf.toString());
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
