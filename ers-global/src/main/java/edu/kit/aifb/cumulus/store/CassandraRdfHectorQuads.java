package edu.kit.aifb.cumulus.store;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

public class CassandraRdfHectorQuads extends AbstractCassandraRdfHector {
	static final String CF_C_SPO = "CSPO";
	static final String CF_REDIRECTS = "Redirects";
	
	transient private final Logger _log = Logger.getLogger(this.getClass().getName()); //CassandraRdfHectorHierHash.class);
	
	public CassandraRdfHectorQuads(String hosts) {
		super(hosts);
		_hosts = hosts;
		_cfs.add(CF_C_SPO);
	}
	
	@Override
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitions(String keyspace) {
		ColumnFamilyDefinition cspo = createCfDefFlat(CF_C_SPO, null, null, ComparatorType.UTF8TYPE, keyspace);
		ColumnFamilyDefinition redirects = createCfDefFlat(CF_REDIRECTS, null, null, ComparatorType.UTF8TYPE, keyspace);
		return Arrays.asList(cspo, redirects);
	}
	
	// Only supported: ?s ?p ?o :c
	public Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException {
//		_log.info("query: " + Nodes.toN3(query) + " idx: CSPO");

		Iterator<Node[]> it = null;
		
		if (query[0] instanceof Variable && query[1] instanceof Variable && query[2] instanceof Variable && !(query[3] instanceof Variable)) {
			// for now we need to specify a complete map as the
			// reorder methods cannot deal with partial mappings
			int[] map = new int[] { 3, 0, 1, 2 };
			// get complete row
			String startRange = "", endRange = "";
			
			// CSPO has one node as key and three nodes as colname
			Node[] nxKey = new Node[] { query[3] };
			String key = query[3].toN3();
			int colNameTupleLength = 3;
			SliceQuery<String,String,String> sq = HFactory.createSliceQuery(getExistingKeyspace(keyspace), _ss, _ss, _ss)
				.setColumnFamily(CF_C_SPO)
				.setKey(key);
			
			it = new ColumnSliceIterator<String>(sq, nxKey, startRange, endRange, map, limit, colNameTupleLength);

			// cut off the context at the end
			// TODO should be doen in the iterator, add more general reorder methods
			it = Iterators.transform(it, new Function<Node[],Node[]>() {
				@Override
				public Node[] apply(Node[] nx) {
					return Arrays.copyOfRange(nx, 0, 3);
				}
			});
		}
		return it;
	}
	
	public void loadRedirects(InputStream fis, String keyspace) throws IOException, InterruptedException {
		_log.info("bulk loading " + CF_REDIRECTS);
		Iterator<Node[]> nxp = new NxParser(fis);
		
		long start = System.currentTimeMillis();
		int i = 0;
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			if (nx.length >= 2 && nx[0] instanceof Resource && nx[1] instanceof Resource) {
				i++;
				Mutator<String> mutator = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
				mutator.insert(nx[0].toString(), CF_REDIRECTS, HFactory.createStringColumn(nx[1].toString(), ""));
			}
		}
		
		long time = (System.currentTimeMillis() - start);
		_log.info(i + " redirects inserted into " + CF_REDIRECTS + " in " + time + " ms (" + ((double)i / time * 1000) + " tuples/s)");
	}

	public String getRedirect(String from, String keyspace) {
		SliceQuery<String,String,String> msq = HFactory.createSliceQuery(getExistingKeyspace(keyspace), _ss, _ss, _ss);
		msq.setColumnFamily(CF_REDIRECTS);
		
		msq.setKey(from.toString());
		msq.setRange("", "", false, 1);
		QueryResult<ColumnSlice<String,String>> result = msq.execute();
		
		ColumnSlice<String,String> slice = result.get();
		
		if (!slice.getColumns().isEmpty()) {
			return slice.getColumns().get(0).getName();
		}
			
		return from;
	}

//	/**
//	 * aggregates a list of quads in a map, resembling the index structure, e.g., C->SPO->""
//	 * this is helpful because we can then construct a single Mutation for all columns of a particular
//	 * key/supercolumn combination (this is done in batchMutate), instead of one for each triple
//	 * 
//	 * @param cf
//	 * @param nodesList
//	 * @return
//	 */
//	private Map<String,Map<String[],String>> createQuadsMap(String cf, List<Node[]> nodesList) {
//		long start = System.currentTimeMillis();
//		
//		Map<String,Map<String[],String>> quadsMap = new HashMap<String,Map<String[],String>>();
//		
//		for (Node[] nx : nodesList) {				
//			String key = nx[3].toN3();
//			
//			String[] spo = new String[3];
//			spo[0] = nx[0].toN3();
//			spo[1] = nx[1].toN3();
//			spo[2] = nx[2].toN3();
//			
//			Map<String[],String> scs = quadsMap.get(key);
//			if (scs == null) {
//				scs = new HashMap<String[],String>();
//				quadsMap.put(key, scs);
//			}
//			
//			scs.put(spo, "");
//		}
//
//		//_log.fine("quads map from " + nodesList.size() + " quads in " + (System.currentTimeMillis() - start) + "ms");
//		
//		return quadsMap;
//	}
//	
//	private ByteBuffer createKey(Node[] nx) {
//		byte[] s = nx[0].toN3().getBytes();
//		byte[] p = nx[1].toN3().getBytes();
//		byte[] o = nx[2].toN3().getBytes();
//		
//		ByteBuffer key = ByteBuffer.allocate(s.length + p.length + o.length);
//
//		key.put(s);
//		key.put(p);
//		key.put(o);
//		
//		return key;
//	}
	
	@Override
	protected void batchInsert(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {		
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// check for quads
				if (nx.length >= 4) {
					String rowKey = nx[3].toN3();
					String colKey = Nodes.toN3(new Node[] { nx[0], nx[1], nx[2] });
					m.addInsertion(rowKey, cf, HFactory.createStringColumn(colKey, ""));
				}
			}
			m.execute();
		}
	}

	// TM
	@Override
	protected void batchDelete(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {		
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// check for quads
				if (nx.length >= 4) {
					String rowKey = nx[3].toN3();
					String colKey = Nodes.toN3(new Node[] { nx[0], nx[1], nx[2] });
					m.addDeletion(rowKey, cf, colKey, _ss );
				}
			}
			m.execute();
		}
	}

	// TM
	@Override
	protected void batchDeleteRow(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {		
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// check for quads
				if (nx.length >= 4) {
					String rowKey = nx[3].toN3();
					String colKey = Nodes.toN3(new Node[] { nx[0], nx[1], nx[2] });
					m.addDeletion(rowKey, cf);
				}
			}
			m.execute();
		}
	}

	@Override 
	protected void batchRun(String cf, List<Node[]> list, String keyspace){
		//_log.severe("CassandraRdfHectorQuads does not implement batchRun() method");
	}
}
