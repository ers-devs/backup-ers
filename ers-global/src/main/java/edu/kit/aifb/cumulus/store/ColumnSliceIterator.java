package edu.kit.aifb.cumulus.store;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;

import com.google.common.collect.Iterators;


public class ColumnSliceIterator<T> implements Iterator<Node[]> {

	private SliceQuery<T,String,String> _sq;
	private Node[] _key;
	private int[] _map;
	private int _limit;
	private ColumnIterator _it;
	private String _lastColName;
	private int _colInterval = 1000;
	private int _colCount = 0;
	private String _endRange;
	private int _colNameTupleLength;
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	public ColumnSliceIterator(SliceQuery<T,String,String> sq, Node[] key, String startRange, String endRange, int[] map, int limit, int colNameTupleLength) {
		_sq = sq;
		_key = key;
		_map = map;
		_limit = limit;
		_endRange = endRange;
		_colNameTupleLength = colNameTupleLength;
		_it = queryIterator(startRange);
	}
	
	private ColumnIterator queryIterator(String start) {
		if (_colCount > _limit)
			return null;
		
		int cols = Math.min(_colInterval, _limit - _colCount);
//		_log.info("iterator for row " + Nodes.toN3(_key) + " from '" + start + "' to '" + _endRange + "', cols: " + cols + " total: " + _colCount + " limit: " + _limit);
		_sq.setRange(start, _endRange, false, cols);
		QueryResult<ColumnSlice<String,String>> result = _sq.execute();
		List<HColumn<String,String>> list = result.get().getColumns();
		ColumnIterator it = null;
		if (list.size() > 0) {
			int iteratorLimit = list.size();
			if (list.size() < _colInterval)
				_lastColName = null;
			else {
				_lastColName = list.get(list.size() - 1).getName();
				iteratorLimit--;
			}
			_colCount += list.size();
			it = new ColumnIterator(Iterators.limit(list.iterator(), iteratorLimit), _key, _colNameTupleLength, _map);
		}
		return it;
	}
	
	@Override
	public boolean hasNext() {
		if (_it == null)
			return false;
		
		if (!_it.hasNext()) 
			_it = _lastColName != null ? queryIterator(_lastColName) : null;
			
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
