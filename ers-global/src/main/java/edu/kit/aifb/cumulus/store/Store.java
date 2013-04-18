package edu.kit.aifb.cumulus.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.io.PrintWriter;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

import edu.kit.aifb.cumulus.webapp.Listener;

/** 
 * 
 * @author aharth
 */
public abstract class Store {
	
	public class DescribeIterator implements Iterator<Node[]> {

		private Resource m_resource;
		private boolean m_include2Hop;

		// iterator for pattern with resource as subject
		private Iterator<Node[]> m_sit;
		// iterator for hop patterns
		private Iterator<Node[]> m_hit;
		// iterator for pattern with resource as object
		private Iterator<Node[]> m_oit;
		private int m_subjects;
		private int m_objects;
		private String m_keyspace; 
		
		public DescribeIterator(Resource resource, boolean include2Hop, int subjects, int objects, String keyspace) throws StoreException {
			m_resource = resource;
			m_include2Hop = include2Hop;
			m_subjects = subjects;
			m_objects = objects;
			m_keyspace = keyspace;
			
			// start with pattern with resource as subjectk
			m_sit = query(pattern(resource, null, null), m_subjects, keyspace);
		}
		
		@Override
		public boolean hasNext() {
			if (m_sit.hasNext())
				return true;
			
			if (m_hit != null && m_hit.hasNext())
				return true;
			
			if (m_oit != null && m_oit.hasNext())
				return true;
			
			return false;
		}

		@Override
		public Node[] next() {
			if (m_include2Hop && m_hit != null && m_hit.hasNext())
				return m_hit.next();
			
			if (m_sit.hasNext()) {
				Node[] next = m_sit.next();
				
				// if the hop should be included, prime the hop iterator,
				// pattern has the current object as subject
				if (m_include2Hop) {
					try {
						m_hit = query(pattern(next[2], null, null), m_subjects, m_keyspace);
					} catch (StoreException e) {
						e.printStackTrace();
						m_hit = null;
					}
				}
				
				// when the subject iterator is finished and there are no more
				// triples in the hop iterator, get the object iterator
				if (!m_sit.hasNext() && (!m_include2Hop || !m_hit.hasNext())) {
					try {
						m_oit = query(pattern(null, null, m_resource), m_objects, m_keyspace);
					} catch (StoreException e) {
						e.printStackTrace();
						m_oit = null;
					}
				}
				
				return next;
			}
			
			if (m_oit != null && m_oit.hasNext())
				return m_oit.next();
			
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
		
	}
	
	transient private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	public abstract void open() throws StoreException;
	public abstract void close() throws StoreException;
	public abstract int addData(Iterator<Node[]> it, String keyspace) throws StoreException;

	// add (e,p,v,g)
	public abstract int addData(String e, String p, String v, String keyspace);
	// update/replace (e,p,v_old,g) with (e,p,v_new,g);
	public abstract int updateData(String e, String p, String v_old, String v_new, String keyspace);
	// delete (e,p,v,g)  
	public abstract int deleteData(String e, String p, String v, String keyspace);
	// this is called by deleteData(...) and for the "clever" delete() that uses query()
	public abstract void deleteData(Node[] nx, String keyspace);

	// delete all data (if force is true, then delete even if it is not empty) 
	public int dropKeyspace(String keyspace) { 
		return dropKeyspace(keyspace, false);
	}
	public abstract int dropKeyspace(String keyspace, boolean force);
	// create a keyspace / graph 
	public abstract int createKeyspace(String keyspace);
	// test if a keyspace / graph exists
	public abstract boolean existsKeyspace(String keyspace);
	// test if a keyspace / graph is empty or not 
	public abstract boolean emptyKeyspace(String keyspace);
	
	// run transactions 
	public abstract int runTransaction(Transaction t);

	public abstract boolean contains(Node s, String keyspace) throws StoreException;
	
	public abstract Iterator<Node[]> query(Node[] query, String keyspace) throws StoreException;
	public abstract Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException;

	// input: <test> | output: ERS_test
	public static final String encodeKeyspace(String keyspace) { 
		return Listener.DEFAULT_ERS_KEYSPACES_PREFIX.concat(keyspace.replace("<","").replace(">",""));
	}
	// input: ERS_test | output: <test>
	public static final String decodeKeyspace(String keyspace) { 
		return "<" + keyspace.substring(Listener.DEFAULT_ERS_KEYSPACES_PREFIX.length()) + ">";
	}
	
	public int queryEntireKeyspace(String keyspace, PrintWriter out, int limit) { 
		return -1;
	}
	
	public int queryAllKeyspaces(int limit, PrintWriter out) { 
		return -1;
	}
	
	public abstract String getStatus();
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, String keyspace) throws StoreException {
		return describe(resource, include2Hop, -1, -1, keyspace);
	}
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, int subjects, int objects, String keyspace) throws StoreException {
		return new DescribeIterator(resource, include2Hop, subjects, objects, keyspace);
	}
	
	private Node[] pattern(Node s, Node p, Node o) {
		return new Node[] { s == null ? new Variable("s") : s, p == null ? new Variable("p") : p, o == null ? new Variable("o") : o }; 
	}
}
