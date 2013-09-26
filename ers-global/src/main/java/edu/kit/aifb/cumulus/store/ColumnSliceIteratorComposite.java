package edu.kit.aifb.cumulus.store;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.semanticweb.yars.nx.Node;

import com.google.common.collect.Iterators;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.ColumnFactory;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import org.semanticweb.yars.nx.Nodes;


public class ColumnSliceIteratorComposite<T> implements Iterator<Node[]> {

	private SliceQuery<T,Composite,String> _sq;
	private Node[] _key;
	private int[] _map;
	private int _limit;
	private ColumnIteratorComposite _it;
	private Composite _lastColName;
	private int _colInterval = 1000;
	private int _colCount = 0;
	private Composite _endRange;
	private int _colNameTupleLength;

	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public ColumnSliceIteratorComposite(SliceQuery<T,Composite,String> sq, Node[] key, Composite startRange,
                Composite endRange, int[] map, int limit, int colNameTupleLength) {
		_sq = sq;
		_key = key;
		_map = map;
		_limit = limit;
		_endRange = endRange;
		_colNameTupleLength = colNameTupleLength;
		_it = queryIterator(startRange);
	}

	private ColumnIteratorComposite queryIterator(Composite start) {
		if (_colCount > _limit)
			return null;

		int cols = Math.min(_colInterval, _limit - _colCount);

		/*_log.info("iterator for row " + Nodes.toN3(_key) + " from '" + start +
                        "' to '" + _endRange + "', cols: " + cols + " total: "
                        + _colCount + " limit: " + _limit);*/

		_sq.setRange(start, _endRange, false, cols);
		QueryResult<ColumnSlice<Composite,String>> result = _sq.execute();
		List<HColumn<Composite,String>> list = result.get().getColumns();

                /* added to get ID and URN part of results 
                if( list.size() != 0 ) {
                    Composite dummy_ID = new Composite();
                    dummy_ID.addComponent(0, list.get(0).getName().get(0, StringSerializer.get()), ComponentEquality.EQUAL);
                    dummy_ID.addComponent(1, list.get(0).getName().get(1, StringSerializer.get()), ComponentEquality.EQUAL);
                    dummy_ID.addComponent(2, "<ID> \"" + list.get(0).getName().get(0, StringSerializer.get()) + "\" . ",
                            ComponentEquality.EQUAL);
                    HColumn<Composite,String> dummy_ID_column = HFactory.createColumn(dummy_ID,
                            list.get(0).getName().getComponent(0).toString(),
                            CompositeSerializer.get(), StringSerializer.get());
                    list.add(dummy_ID_column);

                    Composite dummy_URN = new Composite();
                    dummy_URN.addComponent(0, list.get(0).getName().get(0, StringSerializer.get()), ComponentEquality.EQUAL);
                    dummy_URN.addComponent(1, list.get(0).getName().get(1, StringSerializer.get()), ComponentEquality.EQUAL);
                    dummy_URN.addComponent(2, "<URN> \"" + list.get(0).getName().get(1, StringSerializer.get()) + "\" . ",
                            ComponentEquality.EQUAL);
                    HColumn<Composite,String> dummy_URN_column = HFactory.createColumn(dummy_URN,
                            list.get(0).getName().getComponent(1).toString(),
                            CompositeSerializer.get(), StringSerializer.get());
                    list.add(dummy_URN_column);
                }
                /* end */

		ColumnIteratorComposite it = null;
		if (list.size() > 0) {
			int iteratorLimit = list.size();
			if (list.size() < _colInterval)
				_lastColName = null;
			else {
				_lastColName = list.get(list.size() - 1).getName();
				iteratorLimit--;
			}
			_colCount += list.size();
			it = new ColumnIteratorComposite(Iterators.limit(list.iterator(), iteratorLimit),
                                _key, _colNameTupleLength, _map);
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
