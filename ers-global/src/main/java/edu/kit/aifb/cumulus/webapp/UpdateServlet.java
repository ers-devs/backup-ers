package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.StringTokenizer;
import java.util.HashMap;

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
		String e = req.getParameter("e"); 
		String p = req.getParameter("p");
		String a = req.getParameter("a");
		// NOTE: we require both v_old and v_new as we allow multiple values for same p => by sending the v_old we can identify with record is asked to be updated
		String v_old = req.getParameter("v_old");
		String v_new = req.getParameter("v_new");
		// some checks
		if( e == null || p == null || v_old == null || v_new == null || a == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass data like 'e=_&p=_&v_old=_&v_new=_&a=_'");
			return;
		}
		if( e.isEmpty() || p.isEmpty() || v_old.isEmpty() || v_new.isEmpty() || a.isEmpty()  ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass non empty data 'e=_&p=_&v_old=_&v_new=_&a=_'");
			return;
		}
		if( !e.startsWith("<") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass a resource (e.g. &lt;resource&gt;) as entity");
			return;
		}
		if( !p.startsWith("<") && !p.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please either resources (e.g. &lt;resource&gt;) or literal (e.g. \"literal\") as property");
			return;
		}
		if( !v_old.startsWith("<") && !v_old.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please either resources (e.g. &lt;resource&gt;) or literal (e.g. \"literal\") as old value");
			return;
		}	
		if( !v_new.startsWith("<") && !v_new.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please either resources (e.g. &lt;resource&gt;) or literal (e.g. \"literal\") as new value");
			return;
		}
		if( !a.startsWith("<") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please either resources (e.g. &lt;resource&gt;) as author");
			return;
		}
	
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		PrintWriter out = resp.getWriter();
		resp.setContentType(formatter.getContentType());
		// do an update here 
		if( crdf.updateData(e,p,v_old,v_new,a.replace("<","").replace(">","")) == -2 ) { 
			out.print("Author " + a + " does not exists.");
		}
		else {
			String msg = "Triple ("+e+","+p+","+v_old+") has been removed.";
			msg = msg.replace("<", "&lt;").replace(">","&gt;");
			out.println(msg);
			msg = "Triple ("+e+","+p+","+v_new+") has been added.";
			msg = msg.replace("<", "&lt;").replace(">","&gt;");
			out.print(msg);
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
