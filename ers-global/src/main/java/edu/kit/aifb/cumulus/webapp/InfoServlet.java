package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.kit.aifb.cumulus.store.Store;

/** 
 * 
 * @author aharth
 */
@SuppressWarnings("serial")
public class InfoServlet extends HttpServlet {
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		PrintWriter out = resp.getWriter();
		resp.setContentType("text/plain");

		ServletContext ctx = getServletContext();
		Store crdf = (Store)ctx.getAttribute(Listener.STORE);
		
		out.println(crdf.getStatus());
		
//		if (ctx.getAttribute(Listener.DATASET_HANDLER) != null) {
//			out.println("configured as data server");
//		}
		
		if (ctx.getAttribute(Listener.PROXY_MODE) != null) {
			out.println("proxy mode enabled");
		}

		/*
		out.println(crdf.getClient().toString());
		
		try {
			out.println("cluster name: " + crdf.getClient().describe_cluster_name());
			out.println("client version: " + crdf.getClient().describe_version());
			out.println("partitioner: " + crdf.getClient().describe_partitioner());

			for (KsDef keyspace : crdf.getClient().describe_keyspaces()) {
				out.println("keyspace: " + keyspace);
			}
		} catch (TException e) {
			e.printStackTrace();
			resp.sendError(500, e.getMessage());
			return;
		} catch (InvalidRequestException e) {
			e.printStackTrace();
			resp.sendError(500, e.getMessage());
			return;
		}
		*/
		
		out.close();
	}
}
