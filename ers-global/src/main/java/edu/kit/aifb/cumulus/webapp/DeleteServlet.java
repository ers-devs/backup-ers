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

// TODO: add keyspace info
@SuppressWarnings("serial")
public class DeleteServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("GET currently not supported, sorry.");
	}

	// accept also POST as there is not possible to submit a delete html form
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		doDelete(req, resp);
	}
	
	@Override
	// just erase one resource 
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
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
		String v = req.getParameter("v");
		String a = req.getParameter("g"); 
		if( e == null || p == null || v == null || a == null ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass data like 'e=_&p=_&v=_&a=_'");
			return;
		}
		if( e.isEmpty() || p.isEmpty() || v.isEmpty() || a.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass non empty data 'e=_&p=_&v=_&a=_'");
			return;
		}
		// add brackets if there were not already
		if( !e.startsWith("<") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please pass a resource (e.g. &lt;resource&gt;) as entity");
			return;
		}
		// some checks
		if( !p.startsWith("<") && !p.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please either resources (e.g. &lt;resource&gt;) or literal (e.g. \"literal\") as property");
			return;
		}
		if( !v.startsWith("<") && !v.startsWith("\"") ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please either resources (e.g. &lt;resource&gt;) or literal (e.g. \"literal\") as value");
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
		// do the deletion here 
		if( crdf.deleteData(e,p,v,a.replace("<","").replace(">","")) == -2 ) { 
			out.println("Author " + a + " does not exist.");
		}
		else { 
			String msg = "Triple ("+e+","+p+","+v+") has been added for author " + a + ".";
			msg = msg.replace("<", "&lt;").replace(">","&gt;");
			out.print(msg);
		}
		_log.info("[dataset] DELETE " + (System.currentTimeMillis() - start) + "ms ");
	}

	@Override
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
