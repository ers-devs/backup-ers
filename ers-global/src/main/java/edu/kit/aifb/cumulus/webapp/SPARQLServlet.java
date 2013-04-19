package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.UnsupportedQueryResultFormatException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.openrdf.sail.NotifyingSailConnection;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFStore;

/** 
 * 
 * @author aharth
 */
@SuppressWarnings("serial")
public class SPARQLServlet extends HttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());
	static SimpleDateFormat RFC822 = new SimpleDateFormat("EEE', 'dd' 'MMM' 'yyyy' 'HH:mm:ss' 'Z", Locale.US);

	protected void sendError(ServletContext ctx, HttpServletRequest req, HttpServletResponse resp, int sc, String msg) {
		resp.setStatus(sc);
		req.setAttribute("javax.servlet.error.status_code",sc);
		req.setAttribute("javax.servlet.error.message", msg);
		try {
			ctx.getNamedDispatcher("error").include(req, resp);
		}
		catch (ServletException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		OutputStream out = resp.getOutputStream();
		ServletContext ctx = getServletContext();

		String query = req.getParameter("query");
		String g = req.getParameter("g"); 
		if( g == null || g.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "please give a non-empty graph name as 'g' parameter");
			return;
		}
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
	        String accept = req.getParameter("accept");
	        if (accept == null) {
	        	accept = req.getHeader("accept");
	        }
	        // XXX @@@ HACK include proper accept header parsing
	        if (accept.contains("application/sparql-results+json")) {
        		accept = "application/sparql-results+json";
	        }
	        _log.info("accept header is " + accept);
		
		CumulusRDFStore store = new CumulusRDFStore(crdf, g);
		
		try {
			store.initialize();

			NotifyingSailConnection conn = store.getConnection();

			Repository repo = new SailRepository(store);
			RepositoryConnection repoConn = repo.getConnection();
			
			Query q = repoConn.prepareQuery(QueryLanguage.SPARQL, query);
			
			if (q instanceof BooleanQuery) {
				boolean qres = ((BooleanQuery)q).evaluate();

				BooleanQueryResultFormat fmt = BooleanQueryResultFormat.forMIMEType(accept);
				if (fmt == null) {
					fmt = BooleanQueryResultFormat.SPARQL;
					accept = "application/sparql-results+xml";
				}
				
				resp.setContentType(accept);
				// stuff's cached for a week
				resp.setHeader("Cache-Control", "public");
				Calendar c = Calendar.getInstance();
				c.add(Calendar.DATE, 7);
				resp.setHeader("Expires", RFC822.format(c.getTime()));

				resp.setHeader("Expires", RFC822.format(c.getTime()));
				resp.setHeader("Access-Control-Allow-Origin", "*");
				resp.setHeader("Vary", "Accept");

				try {
					QueryResultIO.write(qres, fmt, out);
				} catch (UnsupportedQueryResultFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (q instanceof TupleQuery) {		
				TupleQueryResult qres = ((TupleQuery)q).evaluate();
				
				TupleQueryResultFormat fmt = TupleQueryResultFormat.forMIMEType(accept);
				if (fmt == null) {
					fmt = TupleQueryResultFormat.SPARQL;
					accept = "application/sparql-results+xml";
				}

				resp.setContentType(accept);
				// stuff's cached for a week
				resp.setHeader("Cache-Control", "public");
				Calendar c = Calendar.getInstance();
				c.add(Calendar.DATE, 7);
				resp.setHeader("Expires", RFC822.format(c.getTime()));

				resp.setHeader("Expires", RFC822.format(c.getTime()));
				resp.setHeader("Access-Control-Allow-Origin", "*");
				resp.setHeader("Vary", "Accept");
				
				try {
					QueryResultIO.write(qres, fmt, out);
				} catch (TupleQueryResultHandlerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnsupportedQueryResultFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				qres.close();
			} else if (q instanceof GraphQuery) {
				GraphQueryResult qres = ((GraphQuery)q).evaluate();

				RDFFormat fmt = RDFFormat.forMIMEType(accept);
				if (fmt == null) {
					fmt = RDFFormat.RDFXML;
					accept = "application/rdf+xml";
				}

				resp.setContentType(accept);
				// stuff's cached for a week
				resp.setHeader("Cache-Control", "public");
				Calendar c = Calendar.getInstance();
				c.add(Calendar.DATE, 7);
				resp.setHeader("Expires", RFC822.format(c.getTime()));

				resp.setHeader("Expires", RFC822.format(c.getTime()));
				resp.setHeader("Access-Control-Allow-Origin", "*");
				resp.setHeader("Vary", "Accept");
				
				try {
					QueryResultIO.write(qres, fmt, out);
				} catch (RDFHandlerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (UnsupportedRDFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				qres.close();
			}

			conn.close();
			repoConn.close();
		} catch (Exception e) {
			e.printStackTrace();
			_log.severe(e.getMessage());
			resp.sendError(500, e.getMessage());
			return;
		}
		
		out.close();
	}
}
