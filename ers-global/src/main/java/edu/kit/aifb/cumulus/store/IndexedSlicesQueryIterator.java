package edu.kit.aifb.cumulus.store;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.query.QueryResult;

import org.semanticweb.yars.nx.Node;

public class IndexedSlicesQueryIterator implements Iterator<Node[]> {

	private IndexedSlicesQuery<String,String,String> _isq;
	int[] _map;
	private int _limit;
	private String _lastKey = null;
	private int _rowInterval = 2;
	private RowIterator _it;
	private String _cf;
	private Keyspace _keyspace;
	private StringSerializer _ss;
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public IndexedSlicesQueryIterator(IndexedSlicesQuery<String,String,String> isq, int[] map, int limit, String cf, Keyspace keyspace, StringSerializer ss) {
		_isq = isq;
		_map = map;
		_limit = limit;
		_cf = cf;
		_keyspace = keyspace;
		_ss = ss;

		_it = queryIterator("");
	}
	
	private RowIterator queryIterator(String start) {
		_isq.setRowCount(_rowInterval);
		_isq.setStartKey(start);
//		_isq.setReturnKeysOnly();
//		_isq.setRange(start, "", false, _rowInterval);
		_log.finer("isq from '" + start + "', interval: " + _rowInterval);
		QueryResult<OrderedRows<String,String,String>> result = _isq.execute();
		RowIterator it = null;
		if (result.get().getList().size() > 0) {
			List<Row<String,String,String>> list = result.get().getList();
			_log.finer("rows: " + list.size());
			if (list.size() < _rowInterval)
				_lastKey = null;
			else
				_lastKey = list.remove(list.size() - 1).getKey();
			it = new RowIterator(list, _map, _cf, _keyspace, _ss);
//			_log.debug("last key now: " + _lastKey);
		}
		return it;
	}


	@Override
	public boolean hasNext() {
		if (_it == null)
			return false;
		
		if (!_it.hasNext())
			_it = _lastKey != null ? queryIterator(_lastKey) : null;
					
		return _it != null && _it.hasNext();
	}

	@Override
	public Node[] next() {
		return _it.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}

}
