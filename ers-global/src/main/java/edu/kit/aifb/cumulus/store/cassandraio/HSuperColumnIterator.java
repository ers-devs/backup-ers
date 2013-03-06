package edu.kit.aifb.cumulus.store.cassandraio;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class HSuperColumnIterator implements Iterator<Node[]> {

	private Iterator<HSuperColumn<String,String,String>> _scIt;
	private Iterator<HColumn<String,String>> _cIt;
	private Node[] _nx;
	private int[] _map;

	public HSuperColumnIterator(int[] map, Node n0, List<HSuperColumn<String,String,String>> list) {
		_scIt = list.iterator();
		_nx = new Node[3];
		_nx[0] = n0;
		_map = map;
	}
	
	@Override
	public boolean hasNext() {
		if (_scIt.hasNext())
			return true;
		
		if (_cIt == null)
			return false;
		
		if (_cIt.hasNext())
			return true;
		
		return false;
	}

	@Override
	public Node[] next() {
		if (_cIt == null || !_cIt.hasNext()) {
			if (!_scIt.hasNext())
				return null;
			
			HSuperColumn<String,String,String> sc = _scIt.next();
			try {
				_nx[1] = NxParser.parseNode(sc.getName());
			} catch (ParseException e) {
				e.printStackTrace();
				return null;
			}
			
			_cIt = sc.getColumns().iterator();
		}
		
		HColumn<String,String> c = _cIt.next();
		if (c != null) {
			try {
				_nx[2] = NxParser.parseNode(c.getName());
				return Util.reorderReverse(_nx, _map);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove not supported");
	}

}
