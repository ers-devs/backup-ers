package edu.kit.aifb.cumulus.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.io.Writer;
import java.io.IOException;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.store.StoreException;
import edu.kit.aifb.cumulus.webapp.Listener;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.digest.DigestUtils;

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
	public abstract int deleteByRowKey(String e, String keyspace);
	public abstract Iterator<Node[]> getRowIterator(String e, String keyspace);

        // shallow copy an entity if exists
        public abstract int shallowClone(String e_src, String graph_src, String e_dest,
                String graph_dest, String unencoded_graph_src, String unencoded_graph_dest);
         // rollback shallow copy an entity 
        public abstract int deleteShallowClone(String e_src, String graph_src, String e_dest,
                String graph_dest, String unencoded_graph_src, String unencoded_graph_dest);

        // deep copy an entity if exists
        public abstract int deepClone(String e_src, String graph_src, String e_dest,
                String graph_dest);
       // rollback deep copy an entity
        public abstract int deleteDeepClone(String e_dest, String graph_dest);

	// delete all data (if force is true, then delete even if it is not empty) 
	public int dropKeyspace(String keyspace) { 
		return dropKeyspace(keyspace, false);
	}
	public abstract int dropKeyspace(String keyspace, boolean force);
	// create a keyspace / graph 
	public abstract int createKeyspace(String keyspace);
	public abstract int createKeyspaceInit(String keyspace);
	// test if a keyspace / graph exists
	public abstract boolean existsKeyspace(String keyspace);
	// test if a keyspace / graph is empty or not 
	public abstract boolean emptyKeyspace(String keyspace);
	
	// run transactions 
	public abstract int runTransaction(Transaction t);

	public abstract boolean contains(Node s, String keyspace) throws StoreException;
	
	public abstract Iterator<Node[]> query(Node[] query, String keyspace) throws StoreException;
	public abstract Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException;

	 private Node getNode(String value, String varName) throws ParseException {
	        if (value != null && value.trim().length() > 2)
                         return NxParser.parseNode(value);
                 else
                         return new Variable(varName);
         }

	// input: <test> | output: ERS_sha1hexa(<test>)
	public static final String encodeKeyspace(String keyspace) { 
		return Listener.DEFAULT_ERS_KEYSPACES_PREFIX.concat(DigestUtils.shaHex(keyspace));
       	}

	// input: ERS_sha1hexa(test) |USE THE LOOKUP => output: <test>
	public final String decodeKeyspace(String keyspace) { 
		// query the Listener.GRAPHS_NAMES_KEYSPACE to get the decoded/real name
                Node[] query = new Node[3];
                try {
	                query[0] = getNode("\""+keyspace+"\"", "s");
                        query[1] = getNode("\"hashValue\"", "p");
                        query[2] = getNode(null, "o");
                 }
                 catch (ParseException ex) {
			ex.printStackTrace();
			_log.severe("ERS exception: " + ex.getMessage());
                        return null; 
                 }
		try {
			 Iterator<Node[]> it = this.query(query, 1, Listener.GRAPHS_NAMES_KEYSPACE);
        	         if (it.hasNext()) {
				Node[] n = (Node[]) it.next();
				return n[2].toString();
	                 }
		 }
		 catch( StoreException ex ) { 
			ex.printStackTrace(); 
			_log.severe("ERS exception: " + ex.getMessage());
		}	
		 return null;
		/*
	         try {
                         return new String(Hex.decodeHex(keyspace.substring(Listener.DEFAULT_ERS_KEYSPACES_PREFIX.length()).toCharArray()));
                 } catch( DecoderException ex) {
                         ex.printStackTrace();
                         return "ERS: Decode Exception!";
                 } */
	}
	
	public int queryEntireKeyspace(String keyspace, Writer out, int limit) throws IOException { 
		return -1;
	}
	
	public int queryAllKeyspaces(int limit, Writer out) throws IOException { 
		return -1;
	}
	
	public abstract String getStatus();
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, String keyspace) throws StoreException {
		return describe(resource, include2Hop, -1, -1, keyspace);
	}
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, int subjects, int objects, String keyspace) throws StoreException {
		return new DescribeIterator(resource, include2Hop, subjects, objects, keyspace);
	}
	
	protected Node[] pattern(Node s, Node p, Node o) {
		return new Node[] { s == null ? new Variable("s") : s, p == null ? new Variable("p") : p, o == null ? new Variable("o") : o }; 
	}
}
