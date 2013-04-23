package edu.kit.aifb.cumulus.store;

import java.nio.ByteBuffer;
import java.io.PrintWriter;
import java.lang.StringBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.UUID;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.cassandra.serializers.UUIDSerializer; 
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.webapp.Listener;

public class CassandraRdfHectorFlatHash extends CassandraRdfHectorQuads {
	private static final String CF_S_PO = "SPO";
	private static final String CF_O_SP = "OSP";
	private static final String CF_PO_S = "POS";
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	public CassandraRdfHectorFlatHash(String hosts) {
		super(hosts);
		_hosts = hosts;
		_cfs.add(CF_S_PO);
		_cfs.add(CF_O_SP);
		_cfs.add(CF_PO_S);
		_maps.put(CF_S_PO, new int[] { 0, 1, 2 });
		_maps.put(CF_O_SP, new int[] { 2, 0, 1 });
		_maps.put(CF_PO_S, new int[] { 1, 2, 0 });
	}
		
	@Override
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitions(String keyspace) {
		ColumnFamilyDefinition spo = createCfDefFlat(CF_S_PO, null, null, ComparatorType.UTF8TYPE, keyspace);
		ColumnFamilyDefinition osp = createCfDefFlat(CF_O_SP, null, null, ComparatorType.UTF8TYPE, keyspace);
		ColumnFamilyDefinition pos = createCfDefFlat(CF_PO_S, Arrays.asList("!p", "!o"), Arrays.asList("!p"), ComparatorType.BYTESTYPE, keyspace);
		
		ArrayList<ColumnFamilyDefinition> li = new ArrayList<ColumnFamilyDefinition>();
		li.addAll(super.createColumnFamiliyDefinitions(keyspace));
		li.addAll(Arrays.asList(spo, osp, pos));
		
		return li;
	}

	private ByteBuffer createKey(Node[] nx) {
		ByteBuffer key = ByteBuffer.allocate(2 * 8);
		key.put(Util.hash(nx[0].toN3()));
		key.put(Util.hash(nx[1].toN3()));
		key.flip();
		return key;
	}

	@Override
	protected void batchInsert(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {
			super.batchInsert(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				
				ByteBuffer rowKey = createKey(new Node[] { reordered[0], reordered[1] });
				String colKey = reordered[2].toN3();
				m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
				m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
				m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));
			}
			m.execute();
		}
		else {
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				String rowKey = reordered[0].toN3();
				String colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
				m.addInsertion(rowKey, cf, HFactory.createStringColumn(colKey, ""));
			}
			m.execute();
		}
	}
	
	// TM
	@Override
	protected void batchDelete(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {
			super.batchDelete(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				
				ByteBuffer rowKey = createKey(new Node[] { reordered[0], reordered[1] });
				String colKey = reordered[2].toN3();
				// delete the full row 
				m.addDeletion(rowKey.array(), cf);
			}
			m.execute();
		}
		else {
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				String rowKey = reordered[0].toN3();
				String colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
				m.addDeletion(rowKey, cf, colKey, _ss);
			}
			m.execute();
		}
	}

	// TM
	@Override
	protected void batchDeleteRow(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {
			super.batchDeleteRow(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				ByteBuffer rowKey = createKey(new Node[] { reordered[0], reordered[1] });
				String colKey = reordered[2].toN3();
				// delete the full row 
				m.addDeletion(rowKey.array(), cf);
			}
			m.execute();
		}
		else {
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				String rowKey = reordered[0].toN3();
				m.addDeletion(rowKey, cf);
_log.info("Delete full row for " + rowKey + " cf= " + cf);
			}
			m.execute();
		}
	}

	private String selectColumnFamily(Node[] q) {
		if (!isVariable(q[0])) {
			if (!isVariable(q[2]))
				return CF_O_SP; // SO
			else
				return CF_S_PO; // S, SP
		}
		
		// here S is variable
		if (!isVariable(q[2])) {
			if (!isVariable(q[1]))
				return CF_PO_S; // PO
			else
				return CF_O_SP; // O
		}
		
		// here S,O are variables
		
		if (!isVariable(q[1])) 
			return CF_PO_S; // P
		
		// for pattern with no constants, use SPO by default
		return CF_S_PO;
	}

	@Override
	public Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException {
		Iterator<Node[]> it = super.query(query, limit, keyspace);
		if (it != null) {
			return it;
		}
		
		String columnFamily = selectColumnFamily(query);
		int[] map = _maps.get(columnFamily);
		Node[] q = Util.reorder(query, map);

//		_log.info("query: " + Nodes.toN3(query) + " idx: " + columnFamily + " reordered: " + Nodes.toN3(q));
		
		if (isVariable(q[0])) {
			// scan over all
			throw new UnsupportedOperationException("triple patterns must have at least one constant");
		}
		else {
			if (columnFamily.equals(CF_PO_S)) {
				if (isVariable(q[1])) {

					// we use a secondary index for P only when no other constant is given
					IndexedSlicesQuery<byte[],String,String> isq = HFactory.createIndexedSlicesQuery(getExistingKeyspace(keyspace), _bs, _ss, _ss)
						.setColumnFamily(columnFamily)
						.addEqualsExpression("!p", q[0].toN3())
						.setReturnKeysOnly();
					
					it = new HashIndexedSlicesQueryIterator(isq, map, limit, columnFamily, getExistingKeyspace(keyspace));
				}
				else {
					// here we always have a PO lookup, POS (=SPO) is handled by OSP or SPO
					// we retrieve all columns from a single row
					// in POS the keys are hashes, we retrieve P and O from columns !p and !o
					
					ByteBuffer key = createKey(new Node[] { q[0], q[1] });

					SliceQuery<byte[],String,String> sq = HFactory.createSliceQuery(getExistingKeyspace(keyspace), _bs, _ss, _ss)
						.setColumnFamily(columnFamily)
						.setKey(key.array())
						.setRange("!o", "!p", false, 2);
					QueryResult<ColumnSlice<String,String>> res = sq.execute();
					
					if (res.get().getColumns().size() == 0)
						return new ArrayList<Node[]>().iterator();
					
					Node[] nxKey = new Node[2];
					try {
						nxKey[0] = NxParser.parseNode(res.get().getColumnByName("!p").getValue());
						nxKey[1] = NxParser.parseNode(res.get().getColumnByName("!o").getValue());
					}
					catch (ParseException e) {
						e.printStackTrace();
					}

					it = new ColumnSliceIterator<byte[]>(sq, nxKey, "<", "", map, limit, 1);
				}
				
			}
			else {
				String startRange = "", endRange = "";
				if (!isVariable(q[1])) {
					// if there is more than one constant, we need to modify the range
					startRange = q[1].toN3();
					endRange = startRange + "_";
	
					if (!isVariable(q[2])) {
						startRange = Nodes.toN3(new Node[] { q[1], q[2] });
						endRange = startRange;
					}
				}

				// SPO, OSP cfs have one node as key and two nodes as colname
				Node[] nxKey = new Node[] { q[0] };
				String key = q[0].toN3();
				int colNameTupleLength = 2;
				
				SliceQuery<String,String,String> sq = HFactory.createSliceQuery(getExistingKeyspace(keyspace), _ss, _ss, _ss)
					.setColumnFamily(columnFamily)
					.setKey(key);
				
				it = new ColumnSliceIterator<String>(sq, nxKey, startRange, endRange, map, limit, colNameTupleLength);
			}
		}
		return it;
	}

	//TM: added for supporting (?,?,?,g) queries
	//NOTE: do not pass the hashed keyspace !! 
	public int queryEntireKeyspace(String keyspace, PrintWriter out, int limit) { 
		int row_count = ( limit > 100) ? 100 : limit;
		int total_row_count=0;

		// String(row), String(column_name), String(column_value)
		Keyspace k = getExistingKeyspace(this.encodeKeyspace(keyspace)); 
        	RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
	            .createRangeSlicesQuery(k, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
        	    .setColumnFamily("SPO")
		    .setRange(null, null, false, 10)
	            .setRowCount((row_count==1)?2:row_count);		//do this trick because if row_count=1 and only one "null" record, then stucks in a loop
         	String last_key = null;

	        while (true) {
        	    rangeSlicesQuery.setKeys(last_key, null);
	  	    try {
        	    	    QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		            OrderedRows<String, String, String> rows = result.get();
		            Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

	        	    // we'll skip this first one, since it is the same as the last one from previous time we executed
		            if (last_key != null && rowsIterator != null) 
				    rowsIterator.next();   

			    while (rowsIterator.hasNext()) {
				    Row<String, String, String> row = rowsIterator.next();
				    last_key = row.getKey();
			 	    // print even if we just have row_key but none columns ?! 
				    if (row.getColumnSlice().getColumns().isEmpty()) {
				    	/*StringBuffer buf = new StringBuffer(); 
		   		        buf.append(row.getKey()); 
					buf.append(" NULL NULL . "); 
				        buf.append(keyspace + "\n");
  				    	out.println(buf.toString());
					++total_row_count; 
				        if( total_row_count >= limit ) 
						return total_row_count;*/
					continue;
				    }
				    for(Iterator it = row.getColumnSlice().getColumns().iterator(); it.hasNext(); ) { 		
				        HColumn c = (HColumn)it.next(); 
				    	StringBuffer buf = new StringBuffer(); 
		   		        buf.append(row.getKey()); 
					buf.append(" "); 
					buf.append(c.getName()); 
				        buf.append(" "); 
					buf.append(c.getValue());
				        buf.append(keyspace + "\n");
  				    	out.println(buf.toString());
					// because the same row key can contain multiple column records, we consider a row each of them rather than the entire row :) 
					// (see how cumulusrdf flat storage works)
					++total_row_count; 
				        if( total_row_count >= limit ) 
						return total_row_count;
				    }
			     }
	            	     if (rows.getCount() < row_count)
        	             	break;
			} catch(Exception e){ 
				e.printStackTrace(); 
				out.println("Exception message: " + e.getMessage());
				out.println("Exception: " + e.toString());
				return 0;
			}
        	}
		return total_row_count;
	}
	
	// iterate the keyspaces and query each one of them 
	public int queryAllKeyspaces(int limit, PrintWriter out) { 
		int total_rows = 0;
		for (KeyspaceDefinition ksDef : _cluster.describeKeyspaces()) {
			String keyspace = ksDef.getName();
			// only keyspaces that uses as prefix our pre-defined one
			if( ! keyspace.startsWith(Listener.DEFAULT_ERS_KEYSPACES_PREFIX) || 
     			      keyspace.equals(Listener.AUTHOR_KEYSPACE) || 
			      keyspace.equals(Listener.GRAPHS_NAMES_KEYSPACE) ) 
				continue;
			//decode it here (use ERS_graphs to get the un-hashed name)
			String decoded_keyspace = this.decodeKeyspace(keyspace); 
			if (decoded_keyspace == null) 
				continue;
			// query this keyspace
			total_rows += queryEntireKeyspace(decoded_keyspace, out, limit);
		}
		return total_rows;
	}
}
