package edu.kit.aifb.cumulus.store;

import java.util.Iterator;

import org.junit.Test;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class SustainedQueryTest {
	@Test
	public void testSustained() throws StoreException {
		Store crdf = new CassandraRdfHectorFlatHash("141.52.218.118:9160,141.52.218.119:9160,141.52.218.120:9160,141.52.218.121:9160");
		crdf.open();

		Resource r1 = new Resource("http://dbpedia.org/resource/Karlsruhe");
		
		long start = System.currentTimeMillis();
		int totalTriples = 0;
		int prevTriples = -1;
		long intervalStart = System.currentTimeMillis();
		long intervalTriples = 0;
		long interval = 10;
		for (int i = 0; i < 1000000; i++) {
			int triples = 0;
			Iterator<Node[]> it = crdf.describe(r1, false);
			while (it.hasNext()) {
				it.next();
				triples++;
			}

			if (prevTriples >= 0 && triples != prevTriples) {
				System.out.println("count changed! " + prevTriples + " -> " + triples);
			}
			
			prevTriples = triples;
			totalTriples += triples;
			intervalTriples += triples;
			
			if (i > 0 && i % interval == 0) {
				double rate = (double)i / (System.currentTimeMillis() - start) * 1000;
				double rateInterval = (double)interval / (System.currentTimeMillis() - intervalStart) * 1000;
				System.out.println(i + " " + totalTriples + " " + (System.currentTimeMillis() - start) + " (" + rate + ", interval: " + rateInterval + ")");
				intervalStart = System.currentTimeMillis();
				intervalTriples = 0;
			}
		}
		
		crdf.close();
	}
}
