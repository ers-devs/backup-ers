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

import edu.kit.aifb.cumulus.store.cassandraio.Store;
import edu.kit.aifb.cumulus.store.cassandraio.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/** 
 * 
 * @author aharth
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class ReadCIOServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
			
		ServletContext ctx = getServletContext();
		if (req.getCharacterEncoding() == null)
			req.setCharacterEncoding("UTF-8");

		resp.setCharacterEncoding("UTF-8");
		String e = req.getParameter("e");
		_log.info("req " + req.getPathInfo() + " " + req.getQueryString());

		if (e == null || e.isEmpty()) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please give a non-empty entity");
			return;
		}
		if( !e.startsWith("<") ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please give a resource (&lt;resource&gt;) as entity");
			return;
		}
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		int subjects = (Integer)ctx.getAttribute(Listener.TRIPLES_SUBJECT);
		int objects = (Integer)ctx.getAttribute(Listener.TRIPLES_OBJECT);
		Resource resource = new Resource(e);
		
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		PrintWriter out = resp.getWriter();
		int triples = 0;
		try {
			Iterator<Node[]> it = crdf.describe(resource, false, subjects, objects);
			if (it.hasNext()) {
				resp.setContentType(formatter.getContentType());
				triples = formatter.print(it, out);
			}
			else
				sendError(ctx, req, resp, HttpServletResponse.SC_NOT_FOUND, "resource not found");
		} 
		catch (StoreException ex) {
			_log.severe(ex.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ex.getMessage());
		}
		_log.info("[dataset] GET " + resource.toN3() + " " + (System.currentTimeMillis() - start) + "ms " + triples + " t");
	}
	
	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("POST currently not supported, sorry.");
	}	
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("DELETE currently not supported, sorry.");
	}
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
