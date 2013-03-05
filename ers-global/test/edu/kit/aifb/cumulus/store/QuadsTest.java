package edu.kit.aifb.cumulus.store;

import java.util.Iterator;

import junit.framework.TestCase;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

public class QuadsTest extends TestCase {
	protected StringSerializer _ss = StringSerializer.get();
	protected BytesArraySerializer _bs = BytesArraySerializer.get();

//	public void testQuads() throws Exception {
//		String hosts = "localhost:9160";
//		
//		CassandraHostConfigurator config = new CassandraHostConfigurator(hosts);
//		config.setCassandraThriftSocketTimeout(40000);
////		config.setMaxActive(6);
////		config.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK);
//		config.setRetryDownedHostsDelayInSeconds(5);
//		config.setRetryDownedHostsQueueSize(128);
//		config.setRetryDownedHosts(true);
//		
//		// XXX FIXME timeout settings
//		config.setCassandraThriftSocketTimeout(0);
//		config.setMaxWaitTimeWhenExhausted(-1);
//		
////		config.setLoadBalancingPolicy(new RoundRobinBalancingPolicy());
//		config.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
//		
//		Cluster cluster = HFactory.getOrCreateCluster("CassandraRdfHectorHierHash", config);
//
//		System.out.println(cluster);
//
//		Keyspace keyspace = HFactory.createKeyspace("KeyspaceCumulus", cluster);
//
//		System.out.println(keyspace);
//		
//		String cf = "CSPO";
//
//		MultigetSliceQuery<String, String, String> msq = HFactory.createMultigetSliceQuery(keyspace, _ss, _ss, _ss);
//		msq.setColumnFamily(cf);
//		msq.setKeys("http://data.semanticweb.org/organization/institut-aifb-universitaet-karlsruhe/rdf"); //http://example.org/from");
//		msq.setRange("", "", false, 3);
//		QueryResult<Rows<String, String, String>> result_rows = msq.execute();
//		
//		Rows<String, String, String> rows = result_rows.get();
//		
//		Iterator<Row<String, String, String>> it = rows.iterator();	
//		
//		while (it.hasNext()) {
//			Row<String, String, String> r = it.next();
//			
//			System.out.println(r.getKey()); 
//			List<HColumn<String, String>> li = r.getColumnSlice().getColumns(); //.get(0).getName());
//			for (HColumn<String, String> col : li) {
//				System.out.println(col.getName());
//			}
//		}
//	}
	
	public void testRedirect2() throws Exception {
		String hosts = "localhost:9160";
		
		CassandraRdfHectorQuads crdf = new CassandraRdfHectorQuads(hosts);
		
		crdf.open();
		//"http://data.semanticweb.org/organization/institut-aifb-universitaet-karlsruhe/rdf"
		Node[] query = new Node[] { new Variable("s"), new Variable("p"), new Variable("o"), new Resource("http://data.semanticweb.org/organization/institut-aifb-universitaet-karlsruhe/rdf") } ;
		Iterator<Node[]> it = crdf.query(query);
		
		while (it.hasNext()) {
			Node[] nx = it.next();
			System.out.println(Nodes.toN3(nx));
		}
	}
}