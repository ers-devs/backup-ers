package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.CloseableIteration;

import java.util.logging.Logger;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailBase;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Variable;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;

public class CumulusRDFStore extends NotifyingSailBase {

	private Store _crdf;
	private String _keyspace;
	private CumulusRDFValueFactory _factory;
	
	private static final Logger _log = Logger.getLogger(CumulusRDFStore.class.getName());
	
	public CumulusRDFStore(Store crdf, String keyspace) {
		_crdf = crdf;
		_keyspace = keyspace;
		_factory = new CumulusRDFValueFactory();
	}

	@Override
	public CumulusRDFValueFactory getValueFactory() {
		return _factory;
	}

	@Override
	public boolean isWritable() throws SailException {
		return true;
	}

	@Override
	protected NotifyingSailConnection getConnectionInternal() throws SailException {
		return new CumulusRDFStoreConnection(this, _keyspace);
	}

	protected <X extends Exception> CloseableIteration<Statement, X> createStatementIterator(Resource subj, URI pred, Value obj, Resource... contexts) throws SailException {
		Node[] nx = new Node[3];
		if (subj == null)
			nx[0] = new Variable("s");
		else
			nx[0] = _factory.createNode(subj);
		if (pred == null)
			nx[1] = new Variable("p");
		else
			nx[1] = _factory.createNode(pred);
		if (obj == null)
			nx[2] = new Variable("o");
		else
			nx[2] = _factory.createNode(obj);
		
//		_log.debug(Nodes.toN3(nx));
		
		try {
			return new CumulusRDFIterator<X>(_crdf.query(nx, _keyspace), this);
		}
		catch (StoreException e) {
			e.printStackTrace();
			throw new SailException(e);
		}

	}
	@Override
	protected void shutDownInternal() throws SailException {
	}

	protected Store getStore() {
		return _crdf;
	}

}
