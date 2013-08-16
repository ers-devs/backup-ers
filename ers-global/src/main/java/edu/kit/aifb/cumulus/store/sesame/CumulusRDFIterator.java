package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.LookAheadIteration;

import java.util.Iterator;

import org.openrdf.model.Statement;
import org.semanticweb.yars.nx.Node;


public class CumulusRDFIterator<X extends Exception> extends LookAheadIteration<Statement, X> {
	private Iterator<Node[]> m_it;
	private CumulusRDFStore _sail;

	public CumulusRDFIterator(Iterator<Node[]> res, CumulusRDFStore sail) {
		this.m_it = res;

		_sail = sail;
	}

	@Override
	protected Statement getNextElement() throws X {
		if (m_it.hasNext()) {
			Node[] nodes = m_it.next();
			return _sail.getValueFactory().createStatement(nodes);
		}
		return null;
	}

}
