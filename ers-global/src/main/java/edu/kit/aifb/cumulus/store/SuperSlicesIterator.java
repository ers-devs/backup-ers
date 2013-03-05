package edu.kit.aifb.cumulus.store;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.semanticweb.yars.nx.Node;

public class SuperSlicesIterator implements Iterator<Node[]> {

	private SuperSliceQuery<String,String,String,String> _rq;
	private Node _key;
	private int[] _map;
	private HSuperColumnIterator _it;
	private int _scInterval = 10000;
	private String _lastSCName;
	private int _limit;
	private int _scCount = 0;
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public SuperSlicesIterator(SuperSliceQuery<String,String,String,String> rq, Node key, int[] map, int limit) {
		_rq = rq;
		_key = key;
		_map = map;
		_limit = limit;

		_it = queryIterator("");
	}
	
	private HSuperColumnIterator queryIterator(String start) {
		if (_scCount >= _limit)
			return null;
		int scs = Math.min(_scInterval, _limit -_scCount);
//		_log.debug("iterator for " + _key + " from '" + start + "' (scs: " + scs + ", total: " + _scCount + ", limit: " + _limit + ")");
		_rq.setRange(start, "", false, scs);
		QueryResult<SuperSlice<String,String,String>> result = _rq.execute();
		HSuperColumnIterator it = null;
		if (result.get().getSuperColumns().size() > 0) {
			List<HSuperColumn<String,String,String>> list = result.get().getSuperColumns();
			if (list.size() > 0) {
				// if the list size is smaller than the interval, this is the last range
				if (list.size() < _scInterval)
					_lastSCName = null;
				else // otherwise, get the name of the last SC in the list
					_lastSCName = list.get(list.size() - 1).getName();
				_scCount += list.size();
				it = new HSuperColumnIterator(_map, _key, list);
			}
		}
		return it;
	}
	
	@Override
	public boolean hasNext() {
		if (_it == null)
			return false;
		
		if (!_it.hasNext())
			_it = _lastSCName != null ? queryIterator(_lastSCName + " ") : null; // add a character to get all SCs greater than the last
		
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
