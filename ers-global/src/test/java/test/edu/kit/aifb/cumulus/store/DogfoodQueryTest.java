package edu.kit.aifb.cumulus.store;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Iterator;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

public class DogfoodQueryTest extends TestCase {
	public void testQuerySubjPred() throws Exception {
		InputStream os = new FileInputStream("data/dogfood/queries-random.nx");

		CassandraRdfHectorHierHash crdf = new CassandraRdfHectorHierHash("localhost:9160");
		
		crdf.open();
		
		long i = 0;

		NxParser nxp = new NxParser(os);
		
		long start = System.currentTimeMillis();
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			Iterator<Node[]> it = crdf.query(new Node[] { nx[0], new Variable("p"), new Variable("o") });
			while (it.hasNext()) {
				it.next();
			}
			
			i++;
			
			if (i % 1000 == 0) {
				System.out.println(i + " in " +  (System.currentTimeMillis() - start) + " ms");
			}
		}

		crdf.close();
		
		System.out.println(i + " queries evaluated in " + (System.currentTimeMillis() - start) + " ms");
		
		os.close();
	}

}
