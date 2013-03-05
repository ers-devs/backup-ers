package edu.kit.aifb.cumulus.store;

import java.util.Iterator;

import junit.framework.TestCase;
import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class RedirectsTest extends TestCase {
	protected StringSerializer _ss = StringSerializer.get();
	protected BytesArraySerializer _bs = BytesArraySerializer.get();

	public void testRedirect() throws Exception {
		String hosts = "localhost:9160";
		
		CassandraHostConfigurator config = new CassandraHostConfigurator(hosts);
		config.setCassandraThriftSocketTimeout(40000);
//		config.setMaxActive(6);
//		config.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK);
		config.setRetryDownedHostsDelayInSeconds(5);
		config.setRetryDownedHostsQueueSize(128);
		config.setRetryDownedHosts(true);
		
		// XXX FIXME timeout settings
		config.setCassandraThriftSocketTimeout(0);
		config.setMaxWaitTimeWhenExhausted(-1);
		
//		config.setLoadBalancingPolicy(new RoundRobinBalancingPolicy());
		config.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
		
		Cluster cluster = HFactory.getOrCreateCluster("CassandraRdfHectorHierHash", config);

		System.out.println(cluster);

		Keyspace keyspace = HFactory.createKeyspace("KeyspaceCumulus", cluster);

		System.out.println(keyspace);
		
		String cf = "Redirects";
//		
//		Mutator<String> mutator = HFactory.createMutator(keyspace, _ss);
//		mutator.insert("http://example.org/from", cf, HFactory.createStringColumn("http://example.org/to", ""));
//		
//		ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspace);
//		
//		columnQuery.setColumnFamily(cf).setKey("http://example.org/from").setName("http://example.org/to");
//
//		QueryResult<HColumn<String, String>> result = columnQuery.execute();
//
//		System.out.println(result);
//		
//		HColumn<String, String> col = result.get();
//		
//		if (col != null) {
//			System.out.println("name/value: " + col.getName() + " " + col.getValue());
//		}
//		
		MultigetSliceQuery<String, String, String> msq = HFactory.createMultigetSliceQuery(keyspace, _ss, _ss, _ss);
		msq.setColumnFamily(cf);
		msq.setKeys("http://data.semanticweb.org/person/guenter-ladwig"); //http://example.org/from");
		msq.setRange("", "", false, 3);
		QueryResult<Rows<String, String, String>> result_rows = msq.execute();
		
		Rows<String, String, String> rows = result_rows.get();
		
		Iterator<Row<String, String, String>> it = rows.iterator();	
		
		while (it.hasNext()) {
			Row<String, String, String> r = it.next();
			
			System.out.println(r.getKey() + " " + r.getColumnSlice().getColumns().get(0).getName());
		}
	}
	
	public void testRedirect2() throws Exception {
		String hosts = "localhost:9160";
		
		CassandraRdfHectorQuads crdf = new CassandraRdfHectorQuads(hosts);
		
		crdf.open();

		System.out.println(crdf.getRedirect("http://data.semanticweb.org/person/guenter-ladwig"));
	}
}