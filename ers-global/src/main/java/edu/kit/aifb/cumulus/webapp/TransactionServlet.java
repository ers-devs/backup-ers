package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.StringTokenizer;
import java.lang.StringBuffer;
import java.util.HashMap;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
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
import edu.kit.aifb.cumulus.store.Transaction;

import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.FileItemFactory; 
import org.apache.commons.fileupload.DefaultFileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.FileItem;

/** 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class TransactionServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public String getLN() {
	    return Thread.currentThread().getStackTrace()[2].getLineNumber()+"";
	}

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
		StringBuffer resp_msg = new StringBuffer();
		// check that we have a file upload request
		boolean isMultipart = ServletFileUpload.isMultipartContent(req);
		if( ! isMultipart ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no file upload");
			return;
		}
		// Create a factory for disk-based file items
		FileItemFactory factory = new DefaultFileItemFactory(512000, new File("/tmp"));
		// Create a new file upload handler
		ServletFileUpload upload = new ServletFileUpload(factory);
		// Parse the request
		try {
			List<FileItem> items = upload.parseRequest(req);
			//get store
			AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);

			// Process the uploaded items
			Iterator iter = items.iterator();
			String file = ""; 
			String random_n = UUID.randomUUID().toString();
			
			while (iter.hasNext()) {
			    FileItem item = (FileItem) iter.next();
			    if (item.isFormField()) {
				// nothing to be done ... 
			    } else {
				   String fieldName = item.getFieldName();
				   String fileName = item.getName();
				   String contentType = item.getContentType();
				   boolean isInMemory = item.isInMemory();
				   long sizeInBytes = item.getSize();
				   // Process a file upload
				   InputStream uploadedStream = item.getInputStream();
				   // write the inputStream to a FileOutputStream
			           file = "/tmp/ERS_transaction_"+random_n;
  			  	   OutputStream out = new FileOutputStream(new File(file));
				   int read = 0;
				   byte[] bytes = new byte[1024];
				   while ((read = uploadedStream.read(bytes)) != -1) {
					out.write(bytes, 0, read);
				   }
				   uploadedStream.close();
				   out.flush();
			  	   out.close();
				   resp_msg.append( "[dataset] POST transaction load " + fileName +", size " + sizeInBytes );
				}
			}
			// now we have the file on disk; if there are failures during the transactions, they can be replayed
			// read one transaction from file 
			BufferedReader bf = new BufferedReader(new FileReader(file));
			int counter = 0;
			int r;
			Transaction t = null;
			while (true) { 
				String line = bf.readLine();
				//_log.info("LINE: " + line);
				if( line == null ) 
					break;
				line = line.trim();
				if( line.equals("BEGIN") ) { 
					// new T starts here 
					++counter;
					t = new Transaction(random_n+"_"+counter);
				}
				else if( line.equals("COMMIT") || line.equals("ROLLBACK") ) { 
					// T ends here
					r = crdf.runTransaction(t); 
					if ( r != 0 ) {
						_log.info("Error running transaction " + t.ID + " error: " + r);
						resp_msg.append("\nERROR running transaction " + t.ID + " error: " + r);
					}
					else 		
						resp_msg.append("\nTransaction " + t.ID + " ran successfully.");
					t = null;
				}
				else if ( line.length() > 10 ) { 
					// this must contain a line with an operation (insert, update, etc) 
					r = t.addOp(line);
					if( r != 0 ) { 
						_log.info("Adding operation to transaction " + t.ID + " failed!");
						_log.info("Returning value: " + r);
						out_r.println("Adding operation to transwaction " + t.ID + " failed!" + "(line: " + line);
						break;
					}
				}
			}
			bf.close();

			// delete the tmp file and write response 
			new File(file).delete();
			resp_msg.append("\nEND TRANSACTIONS");
			out_r.println(resp_msg.toString());
			out_r.close();
			
		} catch(FileUploadException ex) { 
			ex.printStackTrace(); 
			return;
		}	
/*		} catch(StoreException ex) { 
			ex.printStackTrace(); 
			return;
		} */
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
