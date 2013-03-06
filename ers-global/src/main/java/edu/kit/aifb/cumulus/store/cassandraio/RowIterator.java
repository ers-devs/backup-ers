package edu.kit.aifb.cumulus.store.cassandraio;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

class RowIterator implements Iterator<Node[]> {

	private Iterator<Row<String,String,String>> _rows;
	private Iterator<Node[]> _cit;
	private int[] _map;
	private Keyspace _keyspace;
	private StringSerializer _ss;
	private String _cf;
	
	public RowIterator(List<Row<String,String,String>> list, int[] map, String cf, Keyspace keyspace, StringSerializer ss) {
		_rows = list.iterator();
		_map = map;
		_cf = cf;
		_keyspace = keyspace;
		_ss = ss;
	}
	
	@Override
	public boolean hasNext() {
		if (_cit == null || !_cit.hasNext()) {
			if (!_rows.hasNext())
				return false;
			
			Row<String,String,String> row = _rows.next();
			
			Node[] key = null;
			try {
				key = NxParser.parseNodes(row.getKey());
			}
			catch (ParseException e) {
				e.printStackTrace();
			}
			
			 SliceQuery<String,String,String> sq = HFactory.createSliceQuery(_keyspace, _ss, _ss, _ss)
					.setColumnFamily(_cf)
					.setRange("", "", false, Integer.MAX_VALUE)
					.setKey(row.getKey());
			 sq.execute();
			_cit = new ColumnIterator(row.getColumnSlice().getColumns().iterator(), key, 1, _map);
		}
		
		return _cit != null && _cit.hasNext();
	}

	@Override
	public Node[] next() {
		return _cit.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}
	
}
