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
import org.semanticweb.yars.nx.Resource;


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
        private Node[] query;
        private boolean uses_P;
        private boolean uses_O;

	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public ColumnSliceIteratorComposite(SliceQuery<T,Composite,String> sq, Node[] key, Composite startRange,
                Composite endRange, int[] map, int limit, int colNameTupleLength, Node[] query) {
		_sq = sq;
		_key = key;
		_map = map;
		_limit = limit;
		_endRange = endRange;
		_colNameTupleLength = colNameTupleLength;
		_it = queryIterator(startRange);

                this.query = new Node[3];
                for( int i=0; i<query.length; ++i)
                    this.query[i] = query[i];
                uses_P = false;
                if( this.query[1] != null && ! this.query[1].toString().equals("p") )
                    uses_P = true;
                uses_O = false;
                if( this.query[2] != null && ! this.query[2].toString().equals("o") )
                    uses_O = true;
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
           /*Node[] empty = new Node[3];
            while( true ) {
                if( !_it.hasNext() )
                    return empty;
                Node[] n = _it.next();
                if( ! filter(n) )
                    return n;
            }*/
            return filter(_it.next());
	}

        private Node[] filter(Node[] entity) {
            Node[] empty = new Node[5];
            Node[] result = new Node[5];
            // filter by S as in case of using OSP column family and no ID and no URN it may appear false positives
            String s = entity[0].toString().substring(0, entity[0].toString().length()-4);
            result[0] = new Resource(s);
            result[1] = entity[1];
            result[2] = entity[2];
            result[3] = entity[3];
            result[4] = entity[4];

            if( ! s.equals(query[0].toString()) )
                return empty;
           
            // filter by P and O if they were set in the query
            // filter at output as we cannot query directly (SP?) or (SPO) due to URN and ID
            if( uses_P && ! entity[1].toString().equals(query[1].toString()) )
                return empty;

            if( uses_O ) {
                String o = entity[2].toString();
                if( ! o.equals(query[2].toString()) )
                    return empty;
            }
            // END FILTERING
            return result;
	}

	@Override
	public void remove() {
            throw new UnsupportedOperationException("remove not supported");
	}

}
