package edu.kit.aifb.cumulus.store;

import java.util.Iterator;

import me.prettyprint.hector.api.beans.HColumn;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.logging.Logger;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;

public class ColumnIteratorComposite implements Iterator<Node[]> {

	private PeekingIterator<HColumn<Composite,String>> _cols;
	private Node[] _key;
	private int[] _map;
	private int _colNameTupleLength;
//	private int _limit;
//	private int _returned = 0;

        private final Logger _log = Logger.getLogger(this.getClass().getName());

	public ColumnIteratorComposite(Iterator<HColumn<Composite,String>> it, Node[] key, int colNameTupleLength, int[] map) {
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
		HColumn<Composite,String> col = _cols.next();

                int extra_info = 2;
		Node[] nx = new Node[_key.length + _colNameTupleLength + extra_info];
		for (int i = 0; i < _key.length; i++)
			nx[i] = _key[i];

		try {
                        // ID, URN, payload
                        Composite col_name = col.getName();
                        String ID = "\"" + col_name.get(0, StringSerializer.get()) + "\" <tmp> <tmp> . ";
                        String URN = "\"" + col_name.get(1, StringSerializer.get()) + "\" <tmp> <tmp> . ";
                        String payload = col_name.get(2, StringSerializer.get());

			if (_colNameTupleLength > 1) {
				//Node[] stored = NxParser.parseNodes(col.getName().toString());
                                Node[] stored = NxParser.parseNodes(payload);
				for (int i = 0; i < stored.length; i++)
					nx[_key.length + i] = stored[i];
                                nx[_key.length + stored.length] = NxParser.parseNodes(ID)[0];
                                nx[_key.length + stored.length + 1] = NxParser.parseNodes(URN)[0];
                        }
			else {
				//nx[_key.length] = NxParser.parseNode(col.getName().toString());
                            //TODO: as above !!! 
                                 nx[_key.length] = NxParser.parseNode(payload);
                                 nx[_key.length + 1] = NxParser.parseNodes(ID)[0];
                                 nx[_key.length + 2] = NxParser.parseNodes(URN)[0];
                        }
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