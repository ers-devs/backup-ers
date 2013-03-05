package edu.kit.aifb.cumulus.sesame;

import java.io.IOException;

import org.junit.Test;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.query.resultio.UnsupportedQueryResultFormatException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;

import com.sun.xml.internal.ws.encoding.ContentType;

import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFStore;
import edu.kit.aifb.cumulus.store.sesame.SPARQLResultsNxWriterFactory;

public class SesameOutputTest {
	
	@Test
	public void testMediaType() throws Exception {
		System.out.println(TupleQueryResultFormat.forMIMEType("text/plain"));
		System.out.println(TupleQueryResultFormat.forMIMEType("text/csv"));
		System.out.println(TupleQueryResultFormat.forMIMEType("text/comma-seperated-values"));
	}

	@Test
	public void testSesame() throws StoreException, SailException, RepositoryException, MalformedQueryException, QueryEvaluationException {
		CassandraRdfHectorFlatHash crdf = new CassandraRdfHectorFlatHash("localhost:9160", "KeyspaceCumulus");
		crdf.open();
		
		CumulusRDFStore store = new CumulusRDFStore(crdf);
		store.initialize();
		
		NotifyingSailConnection conn = store.getConnection();
		
		Repository repo = new SailRepository(store);
		RepositoryConnection repoConn = repo.getConnection();
		
		//String qstr = "CONSTRUCT { ?o <http://xmlns.com/foaf/0.1/name> \"Andreas Harth\" . ?p <http://swrc.ontoware.org/ontology#author> ?o .} WHERE { ?o <http://xmlns.com/foaf/0.1/name> \"Andreas Harth\" . ?p <http://swrc.ontoware.org/ontology#author> ?o .}";
		String qstr = "SELECT * WHERE { ?o <http://xmlns.com/foaf/0.1/name> \"Andreas Harth\" . ?p <http://swrc.ontoware.org/ontology#author> ?o . ?o ?pred ?obj . }";
		
		Query q = repoConn.prepareQuery(QueryLanguage.SPARQL, qstr);
		
		String mediatype = "text/plain;charset=utf-8";
		ContentType ct = new ContentType(mediatype);
		System.out.println(ct.getPrimaryType() + "/" + ct.getSubType());
		
		if (q instanceof BooleanQuery) {
			BooleanQueryResultFormat fmt = BooleanQueryResultFormat.forMIMEType(mediatype);
			
			boolean qres = ((BooleanQuery)q).evaluate();

			try {
				QueryResultIO.write(qres, fmt, System.out);
			} catch (UnsupportedQueryResultFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (q instanceof TupleQuery) {		
			TupleQueryResult qres = ((TupleQuery)q).evaluate();
			
			TupleQueryResultFormat.register(SPARQLResultsNxWriterFactory.NX);
			TupleQueryResultWriterRegistry.getInstance().add(new SPARQLResultsNxWriterFactory());
			mediatype = "text/nx";
			
			TupleQueryResultFormat fmt = TupleQueryResultFormat.forMIMEType(mediatype);

			try {
				QueryResultIO.write(qres, fmt, System.out);
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

			RDFFormat fmt = RDFFormat.forMIMEType(mediatype);

			try {
				QueryResultIO.write(qres, fmt, System.out);
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
		
		store.shutDown();
		crdf.close();
	}

}
