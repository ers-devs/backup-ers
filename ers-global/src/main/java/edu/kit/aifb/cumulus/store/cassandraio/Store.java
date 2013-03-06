package edu.kit.aifb.cumulus.store.cassandraio;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

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
		
		public DescribeIterator(Resource resource, boolean include2Hop, int subjects, int objects) throws StoreException {
			m_resource = resource;
			m_include2Hop = include2Hop;
			m_subjects = subjects;
			m_objects = objects;
			
			// start with pattern with resource as subject
			m_sit = query(pattern(resource, null, null), m_subjects);
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
						m_hit = query(pattern(next[2], null, null), m_subjects);
					} catch (StoreException e) {
						e.printStackTrace();
						m_hit = null;
					}
				}
				
				// when the subject iterator is finished and there are no more
				// triples in the hop iterator, get the object iterator
				if (!m_sit.hasNext() && (!m_include2Hop || !m_hit.hasNext())) {
					try {
						m_oit = query(pattern(null, null, m_resource), m_objects);
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
	public abstract int addData(Iterator<Node[]> it) throws StoreException;

	// TM
	// add (e,p,v)
	public abstract int addData(String e, String p, String v);
	// update/replace (e,p,v_old) with (e,p,v_new);
	public abstract int updateData(String e, String p, String v_old, String v_new);
	// delete (e,p,v)  
	public abstract int deleteData(String e, String p, String v);

	public abstract boolean contains(Node s) throws StoreException;
	
	public abstract Iterator<Node[]> query(Node[] query) throws StoreException;
	public abstract Iterator<Node[]> query(Node[] query, int limit) throws StoreException;
	
	public abstract String getStatus();
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop) throws StoreException {
		return describe(resource, include2Hop, -1, -1);
	}
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, int subjects, int objects) throws StoreException {
		if (resource.toString().startsWith("http://sws.geonames.org"))
			resource = new Resource(resource.toString() + "/");
//		_log.info("describe resource: " + resource + ", 2hop: " + include2Hop);
		
		return new DescribeIterator(resource, include2Hop, subjects, objects);
	}
	
	private Node[] pattern(Node s, Node p, Node o) {
		return new Node[] { s == null ? new Variable("s") : s, p == null ? new Variable("p") : p, o == null ? new Variable("o") : o }; 
	}
}
