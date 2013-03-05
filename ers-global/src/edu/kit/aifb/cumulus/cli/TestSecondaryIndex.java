package edu.kit.aifb.cumulus.cli;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import edu.kit.aifb.cumulus.store.MurmurHash3;

public class TestSecondaryIndex {

	private static Cluster _cluster;
	private static Keyspace _keyspace;
	protected static StringSerializer _ss = StringSerializer.get();
	protected static BytesArraySerializer _bs = BytesArraySerializer.get();

	protected static KeyspaceDefinition createKeyspaceDefinition() {
		ColumnFamilyDefinition spo = createCfDef("S", Arrays.asList("p"), Arrays.asList("p"), _ss);
		ColumnFamilyDefinition osp = createCfDef("B", Arrays.asList("p"), Arrays.asList("p"), _bs);
		return HFactory.createKeyspaceDefinition("KS", "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(spo, osp));
	}

	protected static <T> ColumnDefinition createColDef(String colName, String validationClass, boolean indexed) {
		BasicColumnDefinition colDef = new BasicColumnDefinition();
		colDef.setName(_ss.toByteBuffer(colName));
		colDef.setValidationClass(validationClass);
		if (indexed) {
			colDef.setIndexType(ColumnIndexType.KEYS);
			colDef.setIndexName(colName + "_index");
		}
		return colDef;
	}

	private static <T> ColumnFamilyDefinition createCfDef(String cfName, List<String> cols, List<String> indexedCols, Serializer<T> vs) {
		BasicColumnFamilyDefinition cfdef = new BasicColumnFamilyDefinition();
		cfdef.setKeyspaceName("KS");
		cfdef.setName(cfName);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setComparatorType(ComparatorType.UTF8TYPE);
		cfdef.setKeyValidationClass(vs.getComparatorType().getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

		if (cols != null)
			for (String colName : cols)
				cfdef.addColumnDefinition(createColDef(colName, vs.getComparatorType().getClassName(), indexedCols.contains(colName)));
		
		return new ThriftCfDef(cfdef);
	}

	public static void main(String[] args) {
		CassandraHostConfigurator config = new CassandraHostConfigurator("localhost:9160");
		config.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
		_cluster = HFactory.getOrCreateCluster("CassandraRdfHectorHierHash", config);
		
		boolean found = false;
		for (KeyspaceDefinition ksDef : _cluster.describeKeyspaces()) {
			if (ksDef.getName().equals("KS")) {
				found = true;
				break;
			}
		}
		
		if (!found)
			_cluster.addKeyspace(createKeyspaceDefinition());
		
		_keyspace = HFactory.createKeyspace("KS", _cluster, new ConsistencyLevelPolicy() {
			@Override
			public HConsistencyLevel get(OperationType arg0, String arg1) {
				return HConsistencyLevel.ONE;
			}
			
			@Override
			public HConsistencyLevel get(OperationType arg0) {
				return HConsistencyLevel.ONE;
			}
		});

		for (int i = 0; i < 500000; i++) {
			String skey = "aaböviwinoinoinoinöonöoiboerubeiorgoijwgoinwefonwoeifwoiefjwefivwlejflwfkjwofijwoiefjwöfijwöefohweofihweffowihefowinfowinfoiwnefwoifnwoeifnwoiefnwoiefnw" + i;
			ByteBuffer bkey = ByteBuffer.allocate(8).putLong(MurmurHash3.MurmurHash3_x64_64(skey.getBytes(), 9001));
			
			Mutator<String> m = HFactory.createMutator(_keyspace, _ss);
			m.addInsertion(skey, "S", HFactory.createStringColumn("p", "pppp"));
			m.addInsertion(skey, "S", HFactory.createStringColumn("name", "valuebepiänboenrboeorgjpiergjepgjreprjgoerjgpeompenbpenbäopertnbäiotrnboetnbäpetbneptbnnbpwef"));
			m.execute();
			
			Mutator<byte[]> m2 = HFactory.createMutator(_keyspace, _bs);
			m2.addInsertion(bkey.array(), "B", HFactory.createStringColumn("p", "pppp"));
			m2.addInsertion(bkey.array(), "B", HFactory.createStringColumn("name", "valuebepiänboenrboeorgjpiergjepgjreprjgoerjgpeompenbpenbäopertnbäiotrnboetnbäpetbneptbnnbpwef"));
			m2.execute();
			
			if (i % 1000 == 0)
				System.out.println(i);
		}

		_cluster.getConnectionManager().shutdown();
		
		System.exit(-1);
	}

}
