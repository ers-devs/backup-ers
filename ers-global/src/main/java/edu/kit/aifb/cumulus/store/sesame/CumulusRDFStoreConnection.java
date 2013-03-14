package edu.kit.aifb.cumulus.store.sesame;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.ConstantOptimizer;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelTreePrinter;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailConnectionBase;
import org.semanticweb.yars.nx.Node;

import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;

public class CumulusRDFStoreConnection extends NotifyingSailConnectionBase {

	private class CumulusRDFTripleSource implements TripleSource {
		@Override
		public CloseableIteration<? extends Statement,QueryEvaluationException> getStatements(Resource subj, URI pred, Value obj, Resource... contexts) throws QueryEvaluationException {
			try {
				return _sail.createStatementIterator(subj, pred, obj, contexts);
			}
			catch (SailException e) {
				e.printStackTrace();
				throw new QueryEvaluationException(e);
			}
		}

		@Override
		public ValueFactory getValueFactory() {
			return _factory;
		}
		
	}
	
	private CumulusRDFStore _sail;
	private Store _crdf;
	private String _keyspace;
	private CumulusRDFValueFactory _factory;
	
	private static final Logger _log = Logger.getLogger(CumulusRDFStoreConnection.class.getName());

	public CumulusRDFStoreConnection(CumulusRDFStore sail, String keyspace) {
		super(sail);
		_sail = sail;
		_crdf = sail.getStore();
		_keyspace = keyspace;
		_factory = sail.getValueFactory();
	}
	
	@Override
	protected void closeInternal() throws SailException {
	}

	@Override
	protected CloseableIteration<? extends BindingSet,QueryEvaluationException> evaluateInternal(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean includeInferred) throws SailException {
//		Lock stLock = _sail.getStatementsReadLock();
		// Clone the tuple expression to allow for more aggressive optimizations
		tupleExpr = tupleExpr.clone();		
		
		if (!(tupleExpr instanceof QueryRoot)) {
			// Add a dummy root node to the tuple expressions to allow the
			// optimizers to modify the actual root node
			tupleExpr = new QueryRoot(tupleExpr);
		}

		TripleSource tripleSource = new CumulusRDFTripleSource();
		EvaluationStrategy strategy = new EvaluationStrategyImpl(tripleSource, dataset);
		new BindingAssigner().optimize(tupleExpr, dataset, bindings);
		new ConstantOptimizer(strategy).optimize(tupleExpr, dataset, bindings);
		new CompareOptimizer().optimize(tupleExpr, dataset, bindings);
		new ConjunctiveConstraintSplitter().optimize(tupleExpr, dataset, bindings);
		new DisjunctiveConstraintOptimizer().optimize(tupleExpr, dataset, bindings);
		new SameTermFilterOptimizer().optimize(tupleExpr, dataset, bindings);
		new QueryModelNormalizer().optimize(tupleExpr, dataset, bindings);
		new IterativeEvaluationOptimizer().optimize(tupleExpr, dataset, bindings);
		new FilterOptimizer().optimize(tupleExpr, dataset, bindings);
		new OrderLimitOptimizer().optimize(tupleExpr, dataset, bindings);

		_log.fine(QueryModelTreePrinter.printTree(tupleExpr));

		CloseableIteration<BindingSet, QueryEvaluationException> iter;
		try {
			iter = strategy.evaluate(tupleExpr, EmptyBindingSet.getInstance());
			return iter;
//			return new LockingIteration<BindingSet, QueryEvaluationException>(stLock, iter);
		}
		catch (QueryEvaluationException e) {
			e.printStackTrace();
			throw new SailException(e);
		}
		finally {
//			stLock.release();
		}
	}

	@Override
	protected CloseableIteration<? extends Resource,SailException> getContextIDsInternal() throws SailException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	protected CloseableIteration<? extends Statement,SailException> getStatementsInternal(Resource subj, URI pred, Value obj, boolean includeInferred, Resource... contexts) throws SailException {
		return _sail.createStatementIterator(subj, pred, obj, contexts);
	}

	@Override
	protected long sizeInternal(Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	protected void startTransactionInternal() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void commitInternal() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void rollbackInternal() throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void addStatementInternal(Resource subj, URI pred, Value obj, Resource... contexts) throws SailException {
		List<Node[]> list = new ArrayList<Node[]>();
		list.add(_factory.createNodes(subj, pred, obj));
		try {
			_crdf.addData(list.iterator(), _keyspace);
		}
		catch (StoreException e) {
			e.printStackTrace();
			throw new SailException(e);
		}
	}

	@Override
	protected void removeStatementsInternal(Resource subj, URI pred, Value obj, Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	protected void clearInternal(Resource... contexts) throws SailException {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	protected CloseableIteration<? extends Namespace,SailException> getNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getNamespaceInternal(String prefix) throws SailException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void setNamespaceInternal(String prefix, String name) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void removeNamespaceInternal(String prefix) throws SailException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void clearNamespacesInternal() throws SailException {
		// TODO Auto-generated method stub
		
	}

}
