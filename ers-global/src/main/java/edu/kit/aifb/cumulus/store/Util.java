package edu.kit.aifb.cumulus.store;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import edu.kit.aifb.cumulus.webapp.Listener;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;

public class Util {
	public static final Charset CHARSET = Charset.forName("UTF-8");

	/**
	 * Reorders <i>nodes</i>, an array in SPO order, to the target order
	 * specified by <i>map</i>.
	 * 
	 * @param nodes
	 * @param map
	 * @return
	 */
	public static Node[] reorder(Node[] nodes, int[] map) {
		Node[] reordered = new Node[map.length];
		for (int i = 0; i < map.length; i++)
			reordered[i] = nodes[map[i]];
		return reordered;
	}

	/**
	 * Reorders <i>nodes</i>, an array in SPO order, to the target order
	 * specified by <i>map</i> + adds the prefix for the inverted property.
	 * 
	 * @param nodes
	 * @param map
	 * @return
	 */
	public static Node[] reorderForLink(Node[] nodes, int[] map) {
		Node[] reordered = new Node[map.length];
		for (int i = 0; i < map.length; i++)
			reordered[i] = nodes[map[i]];
      if( reordered[1] instanceof Resource ) 
         reordered[1] = new Resource(Listener.DEFAULT_ERS_LINKS_PREFIX+reordered[1].toString());
      else if ( reordered[1] instanceof BNode ) 
         reordered[1] = new BNode(Listener.DEFAULT_ERS_LINKS_PREFIX+reordered[1].toString());
      else if ( reordered[1] instanceof Literal ) 
         reordered[1] = new Literal(Listener.DEFAULT_ERS_LINKS_PREFIX+reordered[1].toString());
      else if ( reordered[1] instanceof Variable ) 
         reordered[1] = new Variable(Listener.DEFAULT_ERS_LINKS_PREFIX+reordered[1].toString());
		return reordered;
	}


	/**
	 * Reorders <i>nodes</i> from the order specified by <i>map</i> to SPO
	 * order.
	 * 
	 * @param nodes
	 * @param map
	 * @return
	 */
	public static Node[] reorderReverse(Node[] nodes, int[] map) {
		Node[] reordered = new Node[map.length];
		for (int i = 0; i < map.length; i++)
			reordered[map[i]] = nodes[i];
		return reordered;
	}

	public static long hashLong(String s) {
		return MurmurHash3.MurmurHash3_x64_64(s.getBytes(CHARSET), 9001);
	}
	
	public static ByteBuffer hash(String uri) {
		return (ByteBuffer)ByteBuffer.allocate(8).putLong(MurmurHash3.MurmurHash3_x64_64(uri.getBytes(), 9001)).flip();
//		return ByteBuffer.wrap(m_md.digest(uri.getBytes()), 0, 8);
    }
}
