package edu.kit.aifb.cumulus;

import java.io.FileInputStream;
import java.util.Random;

import junit.framework.TestCase;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

public class CreateQueriesTest extends TestCase {
	public void testCreateQueries() throws Exception {
		FileInputStream fis = new FileInputStream("data/dogfood/data.nt");
		
		NxParser nxp = new NxParser(fis);
		
		Random r = new Random(1);
		
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			
			if (r.nextFloat() < 0.001) {
				System.out.println(Nodes.toN3(nx));
			}
		}
	}
}
