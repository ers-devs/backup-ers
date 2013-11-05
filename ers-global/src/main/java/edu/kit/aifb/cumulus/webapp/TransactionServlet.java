package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.StringTokenizer;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import edu.kit.aifb.cumulus.store.Transaction;

import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.FileItemFactory; 
import org.apache.commons.fileupload.DefaultFileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.FileItem;

/** 
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class TransactionServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public static AtomicInteger tx_counter= new AtomicInteger(0);

	public String getLN() {
	    return Thread.currentThread().getStackTrace()[2].getLineNumber()+"";
	}
	
	private void doPostWithoutFile(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		if (req.getCharacterEncoding() == null)
			req.setCharacterEncoding("UTF-8");
		resp.setCharacterEncoding("UTF-8");

		SerializationFormat formatter = Listener.getSerializationFormat("text/plain");
		PrintWriter out_r = resp.getWriter();
	
		//get the transaction
		String t = req.getParameter("t"); 
		if( t == null || t.isEmpty() ) { 
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the transaction as 't' parameter");
			return;
		}
		// number of retrials in case of a conflict, by default set to 10
		String no_retrials = req.getParameter("retries");
		if( no_retrials == null || no_retrials.isEmpty() ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Please pass the desired number of retrials in"+
				" case the T fails as 'retries' parameter ");
			return;
		}
		int no_retrials_int;
		try { 
			no_retrials_int = Integer.parseInt(no_retrials);
		} catch( NumberFormatException ex ) { 
			// set to default 
			no_retrials_int = 10; 
		}

		StringTokenizer st = new StringTokenizer(t, ";");
		String n;
		Transaction tr = null;
		//_log.info("Process following transaction:");
		if( st.countTokens() < 3 ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "A transaction composed of less than 2 lines it is not possible" +
				" (at least BEGIN, COMMIT/ROLLBACK must exist) ");
			return;
		}
			
		int r;
		Integer resp_msg=0; 
		AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
		while(st.hasMoreTokens()) { 
			n = st.nextToken();
                        // BEGIN-TxType-URN_author
			if( n.startsWith("BEGIN") ) {
				// new T starts here 
				tr = new Transaction(new String("BY_REQ_"+TransactionServlet.tx_counter.incrementAndGet()),
                                        n.substring(n.indexOf("/")+1,n.indexOf("-")));
                                if( n.contains("-") && n.length() > 6 )
                                    // get URN here
                                    tr.setURN(n.substring(n.indexOf("-")+1, n.length()));
                                else
                                    tr.setURN("JUST_DUMMY_URN_FOR_TEST");
			}
			// commit or rollback must end the transaction
			else if( n.equals("COMMIT") || n.equals("ROLLBACK") ) { 
				// T ends here
				while( true ) { 
                                    if( --no_retrials_int < 0 ) {
                                            resp_msg = -1;
                                            break;
                                    }
                                    r = crdf.runTransaction(tr);
                                    if ( r != 0 ) {
                                            _log.info("Error running transaction " + tr.ID + " error: " + r);
                                            //resp_msg.append("\nERROR running transaction " + tr.ID + " error: " + r);
                                            //resp_msg = "1";
                                            resp_msg++;
                                    }
                                    else {
                                            //resp_msg.append("\nTransaction " + tr.ID + " ran successfully.")
                                            //resp_msg = "0";
                                            break;
                                    }
				}
				tr = null;
			}
			else if ( n.length() > 10 ) {
				// this must contain a line with an operation (insert, update, etc) 
				r = tr.addOp(n);
				if( r != 0 ) { 
					_log.info("Adding operation to transaction " + tr.ID + " failed!");
					_log.info("Returning value: " + r);
					out_r.println("Adding operation to transwaction " + tr.ID + " failed!" + "(line: " + n);
                                        out_r.println("Please check the operation type to be one of the supported ones: insert," +
                                                        "update, delete, copy_shallow, copy_deep");
					break;
				}
			}
		}
		// write 0 if successful, -1 if aborted, otherwise the number of retrials
		//sendResponse(ctx, req, resp, HttpServletResponse.SC_OK, resp_msg.toString());
		out_r.println(String.valueOf(resp_msg));
		return;
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
			this.doPostWithoutFile(req, resp); 
			//sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no file upload");
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
                                // BEGIN-TxType-URN_author
				if( line.startsWith("BEGIN") ) {
					// new T starts here 
					++counter;
					//t = new Transaction(random_n+"_"+counter);
                                        t = new Transaction(random_n+"_"+counter, line.substring(line.indexOf("/")+1,
                                                line.indexOf("-")));
                                        if( line.contains("-") && line.length() > 6 )
                                            // get URN here
                                            t.setURN(line.substring(line.indexOf("-")+1, line.length()));
                                        else
                                            t.setURN("JUST_DUMMY_URN_FOR_TEST");
				}
				// commit or rollback must end the transaction
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
						out_r.println("Adding operation to transaction " + t.ID + " failed!" + "(line: " + line);
                                                out_r.println("Please check the operation type to be one of the supported ones: insert," +
                                                        "update, delete, copy_shallow, copy_deep");
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
