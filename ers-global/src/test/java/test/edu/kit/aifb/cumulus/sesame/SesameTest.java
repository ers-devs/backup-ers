package edu.kit.aifb.cumulus.sesame;

import static org.junit.Assert.*;
import info.aduna.iteration.CloseableIteration;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;

import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.store.sesame.CumulusRDFStore;

public class SesameTest {

	@Test
	public void testSesame() throws StoreException, SailException, RepositoryException, MalformedQueryException, QueryEvaluationException {
		CassandraRdfHectorFlatHash crdf = new CassandraRdfHectorFlatHash("localhost:9160", "KeyspaceCumulus");
		crdf.open();
		
		CumulusRDFStore store = new CumulusRDFStore(crdf);
		store.initialize();
		
		NotifyingSailConnection conn = store.getConnection();
		
		Repository repo = new SailRepository(store);
		RepositoryConnection repoConn = repo.getConnection();
		TupleQuery q = repoConn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT * WHERE { ?o <http://xmlns.com/foaf/0.1/name> \"Andreas Harth\" . ?p <http://swrc.ontoware.org/ontology#author> ?o .}");
		TupleQueryResult qres = q.evaluate();
		while (qres.hasNext())
			System.out.println(qres.next().toString());
		qres.close();
		
		conn.close();
		repoConn.close();
		
		
		store.shutDown();
		crdf.close();
	}

}
