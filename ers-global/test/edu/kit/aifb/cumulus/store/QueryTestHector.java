package edu.kit.aifb.cumulus.store;

import java.util.Arrays;
import java.util.Iterator;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;
import org.semanticweb.yars.nx.parser.NxParser;

public class QueryTestHector extends TestCase {
	
	private void printIterator(Iterator<Node[]> it) {
		while (it.hasNext()) {
			System.out.println(Nodes.toN3(it.next()));
		}
	}
	
	public void testQuerySubjPred() throws Exception {
		
		AbstractCassandraRdfHector crdf = new CassandraRdfHectorFlatHash("localhost:9160");
		crdf.open();
		
		Resource r1 = new Resource("http://dbpedia.org/resource/Martina_von_Trapp");
		Resource r2 = new Resource("http://rdf.opiumfield.com/lastfm/friends/pouryascarface");
		Resource r3 = new Resource("http://www.rdfabout.com/rdf/schema/usbill/LegislativeAction");
		
//		Iterator<Node[]> it = crdf.query(new Node[] { r3, new Variable("p"), new Variable("o") });
//		while (it.hasNext()) {	
//			System.out.println(Nodes.toN3(it.next()));
//		}

		Iterator<Node[]> it = crdf.query(new Node[] { r1, new Variable("p"), new Variable("o") });
//		Iterator<Node[]> it = crdf.describe(r1, false);
		int i = 0;
		while (it.hasNext()) {
			Node[] nx = it.next();
			System.out.println(Arrays.toString(nx));
			i++;
		}
		
//		Thread.sleep(30000);
		
		System.out.println(i);
		System.exit(-1);
//
//		it = crdf.query(new Node[] { r1, RDFS.SEEALSO, new Variable("o") });
//		while (it.hasNext()) {
//			System.out.println(Nodes.toN3(it.next()));
//		}
//
//		it = crdf.query(new Node[] { new Variable("s"), RDF.TYPE, r3 });
//		while (it.hasNext()) {
//			System.out.println(Nodes.toN3(it.next()));
//		}
//
//		
		it = crdf.query(new Node[] { r1, new Variable("p"), new Variable("o") });
		while (it.hasNext()) {
			System.out.println(Nodes.toN3(it.next()));
		}

		it = crdf.query(new Node[] { new Resource("http://dbpedia.org/resource/%27Ara"), RDF.TYPE, new Variable("o") });
		while (it.hasNext()) {
			System.out.println(Nodes.toN3(it.next()));
		}
		
//		printIterator(crdf.describe(new Resource("http://dbpedia.org/resource/%27Ara"), false));
//
//		printIterator(crdf.describe(new Resource("http://dbpedia.org/resource/%27Ara"), true));
//
		printIterator(crdf.describe(new Resource("http://www.Department0.University0.edu/FullProfessor1"), true));

		printIterator(crdf.describe(new Resource("http://www.Department0.University0.edu/UndergraduateStudent412"), true));

//		int i = 0;
//		it = crdf.query(new Node[] { new Variable("s"), new Variable("p"), new Variable("o") });
//		while (it.hasNext()) {
//			System.out.println(Nodes.toN3(it.next()));
//			i++;
//		}
//		System.out.println(i);

		crdf.close();
	}

}
