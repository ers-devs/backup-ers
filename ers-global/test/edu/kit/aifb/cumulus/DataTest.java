package edu.kit.aifb.cumulus;
import java.io.UnsupportedEncodingException;

import junit.framework.TestCase;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

public class DataTest extends TestCase {
	private static final String UTF8 = "UTF8";

	public void testRW() throws UnsupportedEncodingException, InvalidRequestException, UnavailableException, TimedOutException {
//		TTransport tr = new TSocket("beta.wunderfacts.com", 9160);
//		TProtocol proto = new TBinaryProtocol(tr);
//		Cassandra.Client client = new Cassandra.Client(proto);
//		tr.open();
//
//		String keyspace = "Keyspace1";
//		String columnFamily = "Standard1";
//		String key = "http://bla.org/";
//
//		// insert data
//		long timestamp = System.currentTimeMillis();
//
//		ColumnPath cpn = new ColumnPath(columnFamily);
//		cpn.setColumn("name".getBytes(UTF8));
//
//		client.insert(keyspace, key, cpn, "Foo F. Bar".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);
//
//		ColumnPath cpa = new ColumnPath(columnFamily);
//		cpa.setColumn("age".getBytes(UTF8));
//
//		client.insert(keyspace, key, cpa, "34".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);
//
//		// read single column
//		System.out.println("single column:");
//		
//		try {
//			Column col = client.get(keyspace, key, cpn, ConsistencyLevel.ONE).getColumn();
//
//			System.out.println("key: " + key);
//			System.out.println("column name: " + new String(col.name, UTF8));
//			System.out.println("column value: " + new String(col.value, UTF8));
//			System.out.println("column timestamp: " + new Date(col.timestamp));
//		} catch (NotFoundException e) {
//			System.err.println(e.getMessage());
//		}
//
//		// read entire row
//		SlicePredicate predicate = new SlicePredicate();
//		SliceRange sliceRange = new SliceRange();
//		sliceRange.setStart(new byte[0]);
//		sliceRange.setFinish(new byte[0]);
//		predicate.setSlice_range(sliceRange);
//
//		System.out.println("\nrow:");
//		ColumnParent parent = new ColumnParent(columnFamily);
//		List<ColumnOrSuperColumn> results = client.get_slice(keyspace, key, parent, predicate, ConsistencyLevel.ONE);
//		for (ColumnOrSuperColumn result : results) {
//			Column column = result.column;
//			System.out.println(new String(column.name, UTF8) + " -> " + new String(column.value, UTF8));
//		}
//		
//		KeyRange kr = new KeyRange();
//		kr.setStart_key("");
//		kr.setEnd_key("");
//
//		ColumnParent cp = new ColumnParent(columnFamily);
//		List<KeySlice> li = client.get_range_slices(keyspace, cp, predicate, kr, ConsistencyLevel.ONE);
//		
//		for (KeySlice ks : li) {
//			System.out.println(ks);
//		}
//		
//		tr.close();
	}
}
