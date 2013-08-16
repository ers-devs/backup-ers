package edu.kit.aifb.cumulus.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

public class CassandraRdfHectorHierHash extends CassandraRdfHectorQuads {
	
	static final String CF_SPO = "SPO";
	static final String CF_POS = "POS";
	static final String CF_OSP = "OSP";
	
	transient private final Logger _log = Logger.getLogger(this.getClass().getName()); //CassandraRdfHectorHierHash.class);
	
	public CassandraRdfHectorHierHash(String hosts) {
		super(hosts);
		_hosts = hosts;
		_cfs.add(CF_SPO);
		_cfs.add(CF_POS);
		_cfs.add(CF_OSP);
		_maps = new HashMap<String,int[]>();
		_maps.put(CF_SPO, new int[] { 0, 1, 2 });
		_maps.put(CF_POS, new int[] { 1, 2, 0 });
		_maps.put(CF_OSP, new int[] { 2, 0, 1 });
	}
		
	@Override
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitions(String keyspace) {
		ColumnFamilyDefinition spo = createCfDefHier(CF_SPO, keyspace);
		ColumnFamilyDefinition pos = createCfDefHier(CF_POS, keyspace);
		ColumnFamilyDefinition osp = createCfDefHier(CF_OSP, keyspace);
		
		ArrayList<ColumnFamilyDefinition> li = new ArrayList<ColumnFamilyDefinition>();
		li.addAll(super.createColumnFamiliyDefinitions(keyspace));
		li.addAll(Arrays.asList(spo, osp, pos));
		return li;
	}
	
	private String selectColumnFamily(Node[] q) {
		if (!isVariable(q[0])) {
			if (isVariable(q[2]))
				return CF_SPO;
			else
				return CF_OSP;
		}
		
		if (!isVariable(q[1])) {
			if (isVariable(q[0]))
				return CF_POS;
			else
				return CF_SPO;
		}
		
		if (!isVariable(q[2])) {
			if (isVariable(q[1]))
				return CF_OSP;
			else
				return CF_POS;
		}
		
		// for pattern with no constants, use SPO by default
		return CF_SPO;
	}

	public Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException {
		Iterator<Node[]> it = super.query(query, limit, keyspace);
		if (it != null) {
			return it;
		}

		// based on the query, find out which column family to use
		String columnFamily = selectColumnFamily(query);
		int[] map = _maps.get(columnFamily);	
		// re-order (s,p,o) based on the column family to be used for querying
		Node[] q = Util.reorder(query, map);
		
//		_log.info("query: " + Nodes.toN3(query) + " idx: " + columnFamily + " reordered: " + Nodes.toN3(q));

		it = new ArrayList<Node[]>().iterator();
		
		if (!(q[0] instanceof Variable)) {
			if (q[1] instanceof Variable) {
				// triple pattern with one constant
				SuperSliceQuery<String,String,String,String> rq = HFactory.createSuperSliceQuery(getExistingKeyspace(keyspace), _ss, _ss, _ss, _ss);
				rq.setColumnFamily(columnFamily)
					.setKey(q[0].toN3());
//					.setRange("", "", false, Integer.MAX_VALUE);
				it = new SuperSlicesIterator(rq, q[0], map, limit);
			} else if (q[2] instanceof Variable) {
				// triple pattern with two constants
				SuperColumnQuery<String,String,String,String> scq = HFactory.createSuperColumnQuery(getExistingKeyspace(keyspace), _ss, _ss, _ss, _ss);
				scq.setColumnFamily(columnFamily)
					.setKey(q[0].toN3())
					.setSuperName(q[1].toN3());
				QueryResult<HSuperColumn<String,String,String>> result = scq.execute();
				it = new HSuperColumnIterator(map, q[0], Arrays.asList(result.get()));
			}
			else {
				// triple pattern with three constants
				SubSliceQuery<String,String,String,String> ssq = HFactory.createSubSliceQuery(getExistingKeyspace(keyspace), _ss, _ss, _ss, _ss);
				ssq.setColumnFamily(columnFamily)
					.setKey(q[0].toN3())
					.setSuperColumn(q[1].toN3())
					.setColumnNames(q[2].toN3());
				QueryResult<ColumnSlice<String,String>> result = ssq.execute();
				List<Node[]> list = new ArrayList<Node[]>();
				if (result.get().getColumns().size() > 0) 
					list.add(q);
				it = list.iterator();
			}
		}
		else {
			throw new UnsupportedOperationException("triple patterns must have at least one constant");
		}
		
		return it;
	}
	
	private Map<String,Map<String,List<String[]>>> createTripleMap(String cf, List<Node[]> nodesList) {
		// aggregates a list of triples in a map, resembling the index structure, e.g., S->P->O
		// this is helpful because we can then construct a single Mutation for all columns of a particular
		// key/supercolumn combination (this is done in batchMutate), instead of one for each triple
		long start = System.currentTimeMillis();
		Map<String,Map<String,List<String[]>>> tripleMap = new HashMap<String,Map<String,List<String[]>>>();
		for (Node[] nx : nodesList) {
			nx = Util.reorder(nx, _maps.get(cf));
			
			String key = nx[0].toN3();
			String scName = nx[1].toN3();
			String cName = nx[2].toN3();
			String cValue = "";
			
			if (nx.length > 3) {
				cValue = nx[3].toN3();
			}
			
			Map<String,List<String[]>> scs = tripleMap.get(key);
			if (scs == null) {
				scs = new HashMap<String,List<String[]>>();
				tripleMap.put(key, scs);
			}
			
			List<String[]> cs = scs.get(scName);
			if (cs == null) {
				cs = new ArrayList<String[]>();
				scs.put(scName, cs);
			}
			
			cs.add(new String[] { cName, cValue });
		}

		//_log.fine("triple map from " + nodesList.size() + " triples in " + (System.currentTimeMillis() - start) + "ms");
		
		return tripleMap;
	}

	@Override
	protected void batchInsert(String cf, List<Node[]> li, String keyspace) {
		if (CF_C_SPO.equals(cf)) {
			super.batchInsert(cf, li, keyspace);
		} else {
			long start = System.currentTimeMillis();
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);

			Map<String,Map<String,List<String[]>>> map = createTripleMap(cf, li);
			for (String key : map.keySet()) {
				Map<String,List<String[]>> superColumns = map.get(key);
				for (String scName : superColumns.keySet()) {
					List<HColumn<String,String>> columns = new ArrayList<HColumn<String,String>>();
					for (String[] c : superColumns.get(scName))
						columns.add(HFactory.createStringColumn(c[0], c[1]));

					HSuperColumn<String,String,String> sc = HFactory.createSuperColumn(scName, columns, _ss, _ss, _ss);
					m.addInsertion(key, cf, sc);
				}
			}
			m.execute();
		}
//		_log.info("mutator create and execute in " + (System.currentTimeMillis() - start) + " ms");
	}
	
	// TM
	@Override
	protected void batchDelete(String cf, List<Node[]> li, String keyspace) {
		if (CF_C_SPO.equals(cf)) {
			super.batchDelete(cf, li, keyspace);
		} else {
			long start = System.currentTimeMillis();
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);

			Map<String,Map<String,List<String[]>>> map = createTripleMap(cf, li);
			for (String key : map.keySet()) {
				Map<String,List<String[]>> superColumns = map.get(key);
				for (String scName : superColumns.keySet()) {
					List<HColumn<String,String>> columns = new ArrayList<HColumn<String,String>>();
					for (String[] c : superColumns.get(scName))
						columns.add(HFactory.createStringColumn(c[0], c[1]));

					HSuperColumn<String,String,String> sc = HFactory.createSuperColumn(scName, columns, _ss, _ss, _ss);
					m.addSubDelete(key, cf, sc);
				}
			}
			m.execute();
		}
//		_log.info("mutator create and execute in " + (System.currentTimeMillis() - start) + " ms");
	}

	// TM
	@Override
	protected void batchDeleteRow(String cf, List<Node[]> li, String keyspace) {
		if (CF_C_SPO.equals(cf)) {
			super.batchDelete(cf, li, keyspace);
		} else {
			long start = System.currentTimeMillis();
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);

			Map<String,Map<String,List<String[]>>> map = createTripleMap(cf, li);
			for (String key : map.keySet()) {
				m.addDeletion(key, cf);
			}
			m.execute();
		}
	}
}
