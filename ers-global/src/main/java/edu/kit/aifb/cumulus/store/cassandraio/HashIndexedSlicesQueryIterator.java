package edu.kit.aifb.cumulus.store.cassandraio;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class HashIndexedSlicesQueryIterator implements Iterator<Node[]> {
	class HashRowIterator implements Iterator<Node[]> {

		private Iterator<Row<byte[],String,String>> _rows;
		private Iterator<Node[]> _cit;
		private int _limit;
		private int _returned = 0;
		
		public HashRowIterator(List<Row<byte[],String,String>> list, int[] map, String cf, int limit) {
			_rows = list.iterator();
			_map = map;
			_limit = limit;
		}
		
		@Override
		public boolean hasNext() {
			if (_cit == null || !_cit.hasNext()) {
				if (!_rows.hasNext())
					return false;
				
				Row<byte[],String,String> row = _rows.next();
				
				SliceQuery<byte[],String,String> sq = HFactory.createSliceQuery(_keyspace, BytesArraySerializer.get(), StringSerializer.get(), StringSerializer.get())
					.setColumnFamily(_cf)
					.setRange("!o", "!p", false, Integer.MAX_VALUE)
					.setKey(row.getKey());
				QueryResult<ColumnSlice<String,String>> res = sq.execute();
				
				Node[] key = new Node[2];
				try {
					key[0] = NxParser.parseNode(res.get().getColumnByName("!p").getValue());
					key[1] = NxParser.parseNode(res.get().getColumnByName("!o").getValue());
				}
				catch (ParseException e) {
					e.printStackTrace();
				}

				_cit = new ColumnSliceIterator<byte[]>(sq, key, "<", "", _map, _limit - _returned, 1);
			}
			
			return _cit != null && _cit.hasNext();
		}

		@Override
		public Node[] next() {
			_returned++;
			return _cit.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
	}
	
	private IndexedSlicesQuery<byte[],String,String> _isq;
	int[] _map;
	private int _limit;
	private byte[] _lastKey = null;
	private int _rowInterval = 50;
	private HashRowIterator _it;
	private String _cf;
	private Keyspace _keyspace;
	private int _returned = 0;
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public HashIndexedSlicesQueryIterator(IndexedSlicesQuery<byte[],String,String> isq, int[] map, int limit, String cf, Keyspace keyspace) {
		_isq = isq;
		_map = map;
		_limit = limit;
		_cf = cf;
		_keyspace = keyspace;

		_it = queryIterator(new byte[0]);
	}
	
	private HashRowIterator queryIterator(byte[] start) {
		_isq.setRowCount(_rowInterval);
		_isq.setStartKey(start);
//		_log.debug("isq interval: " + _rowInterval);
		QueryResult<OrderedRows<byte[],String,String>> result = _isq.execute();
		HashRowIterator it = null;
		if (result.get().getList().size() > 0) {
			List<Row<byte[],String,String>> list = result.get().getList();
//			_log.debug("rows: " + list.size());
			if (list.size() < _rowInterval)
				_lastKey = null;
			else
				_lastKey = list.remove(list.size() - 1).getKey();
			it = new HashRowIterator(list, _map, _cf, _limit - _returned);
//			_log.debug("last key now: " + _lastKey);
		}
		return it;
	}


	@Override
	public boolean hasNext() {
		if (_it == null || _returned >= _limit)
			return false;
		
		if (!_it.hasNext())
			_it = _lastKey != null ? queryIterator(_lastKey) : null;
					
		return _it != null && _it.hasNext();
	}

	@Override
	public Node[] next() {
		_returned++;
		return _it.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}

}
