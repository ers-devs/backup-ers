package edu.kit.aifb.cumulus.store;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.semanticweb.yars.nx.Node;

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
