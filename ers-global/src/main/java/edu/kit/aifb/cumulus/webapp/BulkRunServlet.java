package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.File;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.FileItemFactory; 
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.FileItem;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

/** 
 * @author tmacicas
 * Use this to bulk load/run a file with operations using this pattern: 
 * 	G E P V query_type .  or 
 * 	G E P V_old query_type v_new . 
 * where query_type_number can be 0(query), 1(create), 2(delete), 3(update)
 * e.g.: <e1> <p2> <v3> "0" .
 */
@SuppressWarnings("serial")
public class BulkRunServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		if (req.getCharacterEncoding() == null)
			req.setCharacterEncoding("UTF-8");
		resp.setCharacterEncoding("UTF-8");

		String accept = req.getHeader("Accept");
		SerializationFormat formatter = Listener.getSerializationFormat(accept);
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}

		PrintWriter out_r = resp.getWriter();
		String resp_msg = "";
		// check that we have a file upload request
		boolean isMultipart = ServletFileUpload.isMultipartContent(req);
		if( ! isMultipart ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no file upload");
			return;
		}
		// Create a factory for disk-based file items
		FileItemFactory factory = new DiskFileItemFactory();
		// Create a new file upload handler
		ServletFileUpload upload = new ServletFileUpload(factory);
		// Parse the request
		try {
			List<FileItem> items = upload.parseRequest(req);
			// bulk load options
			int threads = -1;
			String format = "nt";
			int batchSizeMB = 1;
			//get store
			AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
			crdf.setBatchSize(batchSizeMB);

			// Process the uploaded items
			Iterator iter = items.iterator();
			String a =""; String graph ="";
			boolean a_exists = false; 
			String file = ""; 
			
			while (iter.hasNext()) {
			    FileItem item = (FileItem) iter.next();
			    if (item.isFormField()) {
				String name = item.getFieldName();
				String value = item.getString();
				if( name.equals("g") ) { 
					a_exists = true; 
					a = new String(value); 
					// escape if the accept header is not text/plain
					graph = new String(a);
					if ( formatter.getContentType().equals("text/html") ) 
						graph = escapeHtml(a);
				}
			    } else {
				   String fieldName = item.getFieldName();
				   String fileName = item.getName();
				   String contentType = item.getContentType();
				   boolean isInMemory = item.isInMemory();
				   long sizeInBytes = item.getSize();
				   // Process a file upload
				   InputStream uploadedStream = item.getInputStream();
				   // write the inputStream to a FileOutputStream
			           file = "/tmp/upload_bulkload_"+UUID.randomUUID();
  			  	   OutputStream out = new FileOutputStream(new File(file));
				   int read = 0;
				   byte[] bytes = new byte[1024];
				   while ((read = uploadedStream.read(bytes)) != -1) {
					out.write(bytes, 0, read);
				   }
				   uploadedStream.close();
				   out.flush();
			  	   out.close();
				   resp_msg += "Bulkrun" + fileName + ", size " + sizeInBytes;
			           out_r.println(resp_msg); 
				}
			}
			if( ! a_exists || a == null || a.isEmpty() ) { 
				sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass also the graph name as 'g' parameter");
			}
			else { 
				if( !a.startsWith("<") || !a.endsWith(">") ) {
					sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass a resource as the graph name.");
					return;
				}

				// load here 
				// note: if threads==1, then it will be set to the number of hosts
				// note2: one thread would parse the date and on a round-robin manner, the worker threads would run the batches 
				// note3: if there are NOY ONLY inserts, then the results may not be deterministic (as concurrent threads are applying 
				// note4: ONLY INSERTS, DELETES AND UPDATES ARE RUN !! QUERIES, GET ARE IGNORED
				if( crdf.bulkRun(new File(file), format, threads, Store.encodeKeyspace(a)) == 1 ) 
					sendError(ctx, req, resp, HttpServletResponse.SC_CONFLICT, "Graph " + graph + " does not exist yet. Please create it before bulk running operations."); 
				else {
					resp_msg = "Bulkrun execution time: " + (System.currentTimeMillis() - start) + "ms";
					sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, resp_msg); 
					_log.info(resp_msg);
				}
			}
			// delete the tmp file 
			new File(file).delete();
			out_r.println(resp_msg);
			out_r.close();
		} catch(FileUploadException ex) { 
			ex.printStackTrace(); 
			return;
		} catch(StoreException ex) { 
			ex.printStackTrace(); 
			return;
		}
		return;
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("GET currently not supported, sorry.");
	}	
	public void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("DELETE currently not supported, sorry.");
	}
	public void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		throw new UnsupportedOperationException("PUT currently not supported, sorry.");
	}
}
