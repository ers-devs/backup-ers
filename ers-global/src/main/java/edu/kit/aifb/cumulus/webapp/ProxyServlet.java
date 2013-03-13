package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

import edu.kit.aifb.cumulus.store.CassandraRdfHectorQuads;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/** 
 * 
 * @author aharth
 */
@SuppressWarnings("serial")
public class ProxyServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();

		ServletContext ctx = getServletContext();

		// in proxy GETs, *everything* is converted into a store lookup
		// this means that all other URLs (e.g. /info, /error) won't work!
		
		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			resp.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE);
			return;
		}
		
		resp.setCharacterEncoding("UTF-8");

		// we require the host header
		String host = req.getHeader("host");
		if (host == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_IMPLEMENTED, req.getRequestURI() + " " + "\nall requests require Host header (mandatory since HTTP version 1.1)!");
			return;
		}

		String requestURI = req.getRequestURI();

		// reconstruct originally requested resource
		Resource resource = new Resource("http://" + host + URLDecoder.decode(requestURI, "UTF-8"));

		CassandraRdfHectorQuads crdf = (CassandraRdfHectorQuads)ctx.getAttribute(Listener.STORE);
		PrintWriter out = resp.getWriter();
		int triples = 0;
		
		try {
			String from = resource.toString();
			String to = crdf.getRedirect(from);

			if (!from.equals(to)) {
				resp.sendRedirect(to);
				_log.info("[proxy] GET " + resource.toN3() + " REDIRECT " + to + " " + (System.currentTimeMillis() - start) + "ms");
			}
			else {
				Iterator<Node[]> it = crdf.query(new Node[] { new Variable("s"), new Variable("p"), new Variable("o"), resource });
				if (it.hasNext()) {
					resp.setContentType(formatter.getContentType());
					triples = formatter.print(it, out);
				}
				else
					sendError(ctx, req, resp, HttpServletResponse.SC_NOT_FOUND, "resource not found");
				
				_log.info("[proxy] GET " + resource.toN3() + " " + (System.currentTimeMillis() - start) + "ms " + triples + "t");
			}

		} 
		catch (StoreException e) {
			_log.severe(e.getMessage());
			sendError(ctx, req, resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
		}
		
	}
}
