package edu.kit.aifb.cumulus.store;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.google.common.collect.Lists;

import junit.framework.TestCase;

public class LoadTest extends TestCase {
	public void testLoad() throws Exception {
		AbstractCassandraRdfHector crdf = new CassandraRdfHectorFlatHash("localhost:9160");

		crdf.open();
		crdf.bulkLoad(new File("data/dogfood/data.nt"), "nt", 1);
		crdf.close();
		
	}

}
