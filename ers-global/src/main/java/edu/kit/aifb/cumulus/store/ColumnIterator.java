package edu.kit.aifb.cumulus.store;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;

import org.apache.cassandra.cli.CliParser.columnName_return;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class ColumnIterator implements Iterator<Node[]> {

	private PeekingIterator<HColumn<String,String>> _cols;
	private Node[] _key;
	private int[] _map;
	private int _colNameTupleLength;
//	private int _limit;
//	private int _returned = 0;

	public ColumnIterator(Iterator<HColumn<String,String>> it, Node[] key, int colNameTupleLength, int[] map) {
		_cols = Iterators.peekingIterator(it);
		_key = key;
		_colNameTupleLength = colNameTupleLength;
		_map = map;
	}
	
	@Override
	public boolean hasNext() {
		// skip p column in PO_S cf
		if (_cols.hasNext() && (_cols.peek().getName().equals("p") || _cols.peek().getName().equals("!p") || _cols.peek().getName().equals("!o")))
			_cols.next();
		
		return _cols.hasNext();
	}

	@Override
	public Node[] next() {
		HColumn<String,String> col = _cols.next();
		
		Node[] nx = new Node[_key.length + _colNameTupleLength];
		for (int i = 0; i < _key.length; i++)
			nx[i] = _key[i];
		
		try {
			if (_colNameTupleLength > 1) {
				Node[] stored = NxParser.parseNodes(col.getName());
				for (int i = 0; i < stored.length; i++)
					nx[_key.length + i] = stored[i];
				}
			else
				nx[_key.length] = NxParser.parseNode(col.getName());
		}
		catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		
		return Util.reorderReverse(nx, _map);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}
	
}