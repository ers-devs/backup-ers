package edu.kit.aifb.cumulus.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.yaml.snakeyaml.Yaml;

import edu.kit.aifb.cumulus.store.CassandraRdfHectorHierHash;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.ExecuteTransactions;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;

import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.cassandra.service.OperationType;

import edu.kit.aifb.cumulus.webapp.formatter.HTMLFormat;
import edu.kit.aifb.cumulus.webapp.formatter.NTriplesFormat;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import edu.kit.aifb.cumulus.webapp.formatter.StaxRDFXMLFormat;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
/** 
 * 
 * @author aharth
 * @author tmacicas
 */
public class Listener implements ServletContextListener {

	private static final String PARAM_CONFIGFILE = "config-file";
	
	private static final String PARAM_HOSTS = "cassandra-hosts";
	private static final String PARAM_EMBEDDED_HOST = "cassandra-embedded-host";		// this must be set to 'true' if intended to be run as embedded version
	private static final String PARAM_RUN_ON_OPENSHIFT = "run-on-openshift";		// this must be set to 'true' if intended to be run on Openshift platform
	private static final String PARAM_ERS_KEYSPACES_PREFIX = "ers-keyspaces-prefix";
	private static final String PARAM_LAYOUT = "storage-layout";
	private static final String PARAM_PROXY_MODE = "proxy-mode";
//	private static final String PARAM_RESOURCE_PREFIX = "resource-prefix";
//	private static final String PARAM_DATA_PREFIX = "data-prefix";
	private static final String PARAM_TRIPLES_SUBJECT = "triples-subject";
	private static final String PARAM_TRIPLES_OBJECT = "triples-object";
	private static final String PARAM_QUERY_LIMIT = "query-limit";
	private static final String PARAM_TUPLE_LENGTH = "tuple_length";
	private static final String PARAM_DEFAULT_REPLICATION_FACTOR = "default-replication-factor";
	private static final String PARAM_START_EMBEDDED = "start-embedded";
        private static final String PARAM_TRANS_LOCKING_GRANULARITY = "ers-transactional-locking-granularity";
        private static final String PARAM_TRANS_LOCKING_ZOOKEEPER = "ers-transactional-locking-zookeeper";
	
	// add here the params stored in web.xml
	private static final String[] CONFIG_PARAMS = new String[] {
		PARAM_HOSTS, PARAM_EMBEDDED_HOST, PARAM_ERS_KEYSPACES_PREFIX, 
		PARAM_LAYOUT, PARAM_PROXY_MODE, PARAM_RUN_ON_OPENSHIFT,
		//PARAM_RESOURCE_PREFIX, PARAM_DATA_PREFIX,
		PARAM_TRIPLES_OBJECT,
		PARAM_TRIPLES_SUBJECT, PARAM_QUERY_LIMIT,
		PARAM_DEFAULT_REPLICATION_FACTOR, PARAM_START_EMBEDDED,
                PARAM_TRANS_LOCKING_GRANULARITY
		};
	
//	private static final String DEFAULT_RESOURCE_PREFIX = "resource";
//	private static final String DEFAULT_DATA_PREFIX = "data";
	private static final int DEFAULT_TRIPLES_SUBJECT = -1;
	private static final int DEFAULT_TRIPLES_OBJECT = 5000;
	private static final int DEFAULT_QUERY_LIMIT = -1;
        public static final String DEFAULT_ERS_LINKS_PREFIX = "INVERTED_";
        public static final String SHALLOW_COPY_PROPERTY = "\"sameAs\"";
	
	private static final String LAYOUT_SUPER = "super";
	private static final String LAYOUT_FLAT = "flat";
	
	public static String DEFAULT_ERS_KEYSPACES_PREFIX = "ERS_";
	//public static final String AUTHOR_KEYSPACE = "ERS_authors";
	public static final String GRAPHS_NAMES_KEYSPACE = "ERS_graphs";
	private static String DEFAULT_RUN_ON_OPENSHIFT = "no";
        public static String DEFAULT_TRANS_LOCKING_GRANULARITY = "3";

	// NOTE: consistency level is tunable per keyspace, per CF, per operation type 
        // for the moment all keyspaces use this default policy 
	public static ConsistencyLevelPolicy DEFAULT_CONSISTENCY_POLICY = new ConsistencyLevelPolicy() { 
			@Override
                        public HConsistencyLevel get(OperationType op_type, String cf) {
                                /*NOTE: based on operation type and/or column family, the 
                                   consistency level is tunable
                                   However, we just use for the moment the given parameter 
                                */
				if( op_type == OperationType.WRITE ) 
	                                return Listener.write_cons_level;
				else 
					return Listener.read_cons_level;
                        }   
                                                                                                       
                        @Override
                        public HConsistencyLevel get(OperationType op_type) {
				if( op_type == OperationType.WRITE )
	                                return Listener.write_cons_level;
				else
					return Listener.read_cons_level;
                        }   
	};
        public static HConsistencyLevel read_cons_level = HConsistencyLevel.ONE;
        public static HConsistencyLevel write_cons_level = HConsistencyLevel.ONE;

	// NOTE: this can be adjusted per keyspace, the default one is used for now by all of the keyspaces
	// NOTE2: this is a web.xml parameter; use the default value for the Embedded version
	public static Integer DEFAULT_REPLICATION_FACTOR = 1; 

        public static final String SEQ_NUMBER_PROPERTY = "<lastSync>";

	public static final String TRIPLES_SUBJECT = "tsubj";
	public static final String TRIPLES_OBJECT = "tobj";
	public static final String QUERY_LIMIT = "qlimit";

	public static final String ERROR = "error";
	public static final String STORE = "store";
	
	public static final String PROXY_MODE = "proxy-mode";

//	public static final String DATASET_HANDLER = "dataset_handler";
//	public static final String PROXY_HANDLER = "proxy_handler";

	private Store _crdf = null;
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	private static Map<String,String> _mimeTypes = null;
	private static Map<String,SerializationFormat> _formats = null;

        // Zookeeper + Curator stuff
        public static TestingCluster ts;
        public static CuratorFramework curator_client;
        public static int USE_ZOOKEEPER;

	@SuppressWarnings("unchecked")
	public void contextInitialized(ServletContextEvent event) {
		ServletContext ctx = event.getServletContext();
		
		// sesame init register media type
//		TupleQueryResultFormat.register(SPARQLResultsNxWriterFactory.NX);
//		TupleQueryResultWriterRegistry.getInstance().add(new SPARQLResultsNxWriterFactory());

		// parse config file
		String configFile = ctx.getInitParameter(PARAM_CONFIGFILE);
		Map<String,String> config = null;
		if (configFile != null && new File(configFile).exists()) {
			_log.info("config file: " + configFile);
			try {
				Map<String,Object> yaml = (Map<String,Object>)new Yaml().load(new FileInputStream(new File(configFile)));

				// we might get non-String objects from the Yaml file (e.g., Boolean, Integer, ...)
				// as we only get Strings from web.xml (through ctx.getInitParameter) 
				// when that is used for configuration, we convert everything to Strings 
				// here to keep the following config code simple
				config = new HashMap<String,String>();
				for (String key : yaml.keySet())
					config.put(key, yaml.get(key).toString());
			}
			catch (IOException e) {
				e.printStackTrace();
				_log.severe(e.getMessage());
				ctx.setAttribute(ERROR, e);
			}
			if (config == null) {
				_log.severe("config file found at '" + configFile + "', but is empty?");
				ctx.setAttribute(ERROR, "config missing");
				return;
			}
		}
		else {
			_log.info("config-file param not set or config file not found, using parameters from web.xml");
			config = new HashMap<String,String>();
			for (String param : CONFIG_PARAMS) {
				String value = ctx.getInitParameter(param + "-OVERRIDE");

				if (value == null) {
					value = ctx.getInitParameter(param);
				}
				if (value != null) {
					config.put(param, value);
				}
			}
		}
		_log.info("config: " + config);
		
		_mimeTypes = new HashMap<String,String>();
		_mimeTypes.put("application/rdf+xml", "xml");
		_mimeTypes.put("text/plain", "ntriples");
		_mimeTypes.put("text/html", "html");
		_log.info("mime types: "+ _mimeTypes);
		
		_formats = new HashMap<String,SerializationFormat>();
		_formats.put("xml", new StaxRDFXMLFormat());
		_formats.put("ntriples", new NTriplesFormat());
		_formats.put("html", new HTMLFormat());
		
		if (!config.containsKey(PARAM_HOSTS) || !config.containsKey(PARAM_EMBEDDED_HOST) ||
		    !config.containsKey(PARAM_LAYOUT)) {
			_log.severe("config must contain at least these parameters: " + 
				(Arrays.asList(PARAM_HOSTS, PARAM_EMBEDDED_HOST, PARAM_LAYOUT)));
			ctx.setAttribute(ERROR, "params missing");
			return;
		}
		try {
			// NOTE: do not set it > than total number of cassandra instances 
  			// NOTE2: this must be enforeced to 1 if embedded version is used
 			Listener.DEFAULT_REPLICATION_FACTOR = config.containsKey(PARAM_DEFAULT_REPLICATION_FACTOR) ? 
				Integer.parseInt(config.get(PARAM_DEFAULT_REPLICATION_FACTOR)) : Listener.DEFAULT_REPLICATION_FACTOR;
			// all keyspaces created using this system will prepend this prefix
			Listener.DEFAULT_ERS_KEYSPACES_PREFIX = config.containsKey(PARAM_ERS_KEYSPACES_PREFIX) ? 
				config.get(PARAM_ERS_KEYSPACES_PREFIX) : Listener.DEFAULT_ERS_KEYSPACES_PREFIX;
			// this parameter must be set if intended to run the project on Openshift platform
			Listener.DEFAULT_RUN_ON_OPENSHIFT = config.containsKey(PARAM_RUN_ON_OPENSHIFT) ? 
				config.get(PARAM_RUN_ON_OPENSHIFT) : Listener.DEFAULT_RUN_ON_OPENSHIFT;
        
         // if it set and has value from {1,2,3}, then choose one, otherwise use default
         if( config.containsKey(PARAM_TRANS_LOCKING_GRANULARITY) && ( config.get(PARAM_TRANS_LOCKING_GRANULARITY).equals("1") ||
               config.get(PARAM_TRANS_LOCKING_GRANULARITY).equals("2") || config.get(PARAM_TRANS_LOCKING_GRANULARITY).equals("3"))) 
               Listener.DEFAULT_TRANS_LOCKING_GRANULARITY = config.get(PARAM_TRANS_LOCKING_GRANULARITY); 
         
			String hosts="";
			// embedded ?!
			if( config.containsKey(PARAM_START_EMBEDDED) && config.get(PARAM_START_EMBEDDED).equals("yes") ) {
				// force the replication to 1 as, most probably, there will be just one instance of embedded Cassandra running locally
				Listener.DEFAULT_REPLICATION_FACTOR = 1; 
				// start embedded Cassandra
				EmbeddedCassandraServerHelper.startEmbeddedCassandra();
				hosts = config.get(PARAM_EMBEDDED_HOST);
				_log.info("embedded cassandra host: " + hosts);
				if( Listener.DEFAULT_RUN_ON_OPENSHIFT.equals("yes")) { 
					_log.severe("The project cannot be run on Openshift as embedded! Please set either Openshift or Embedded on web.xml and run once again");
					ctx.setAttribute(ERROR, "conflict parameters");
					return;
				}
			}
			// openshift ?!
			else if( Listener.DEFAULT_RUN_ON_OPENSHIFT.equals("yes")) { 	
				// force the replication to 1 as, most probably, one may use just one instance of Openshift
				Listener.DEFAULT_REPLICATION_FACTOR = 1;
				hosts = System.getenv("OPENSHIFT_INTERNAL_IP") + ":19160";
				_log.info("Openshift cassandra host: " + hosts);
			}
			// standalone case 
			else { 
				hosts = config.get(PARAM_HOSTS);
				_log.info("Cassandra host: " + hosts); 
			}
			String layout = config.get(PARAM_LAYOUT);
			
			_log.info("ers keyspaces prefix: " + Listener.DEFAULT_ERS_KEYSPACES_PREFIX );
			_log.info("storage layout: " + layout);
                        _log.info("transactional locking granularity level: " + Listener.DEFAULT_TRANS_LOCKING_GRANULARITY);
			
			if (LAYOUT_SUPER.equals(layout))
				_crdf = new CassandraRdfHectorHierHash(hosts);
			else if (LAYOUT_FLAT.equals(layout))
				_crdf = new CassandraRdfHectorFlatHash(hosts);
			else
				throw new IllegalArgumentException("unknown storage layout");
			// set some cluster wide parameters 
			_crdf.open();
   			// create the Authors keyspace
			//_crdf.createKeyspaceInit(AUTHOR_KEYSPACE);
			// create the Graph names keyspace
			_crdf.createKeyspaceInit(GRAPHS_NAMES_KEYSPACE);
			ctx.setAttribute(STORE, _crdf);
		} catch (Exception e) {
			_log.severe(e.getMessage());
			e.printStackTrace();
			ctx.setAttribute(ERROR, e);
		}
		int subjects = config.containsKey(PARAM_TRIPLES_SUBJECT) ?
				Integer.parseInt(config.get(PARAM_TRIPLES_SUBJECT)) : DEFAULT_TRIPLES_SUBJECT;
		int objects = config.containsKey(PARAM_TRIPLES_OBJECT) ?
				Integer.parseInt(config.get(PARAM_TRIPLES_OBJECT)) : DEFAULT_TRIPLES_OBJECT;
		int queryLimit = config.containsKey(PARAM_QUERY_LIMIT) ?
				Integer.parseInt(config.get(PARAM_QUERY_LIMIT)) : DEFAULT_QUERY_LIMIT;

		subjects = subjects < 0 ? Integer.MAX_VALUE : subjects;
		objects = objects < 0 ? Integer.MAX_VALUE : objects;
		queryLimit = queryLimit < 0 ? Integer.MAX_VALUE : queryLimit;
				
		_log.info("subject triples: " + subjects);
		_log.info("object triples: " + objects);
		_log.info("query limit: " + queryLimit);
		_log.info("default replication level: " + Listener.DEFAULT_REPLICATION_FACTOR);

		ctx.setAttribute(TRIPLES_SUBJECT, subjects);
		ctx.setAttribute(TRIPLES_OBJECT, objects);
		ctx.setAttribute(QUERY_LIMIT, queryLimit);
		
		if (config.containsKey(PARAM_PROXY_MODE)) {
			boolean proxy = Boolean.parseBoolean(config.get(PARAM_PROXY_MODE));
			if (proxy)
				ctx.setAttribute(PROXY_MODE, true);
		}


                USE_ZOOKEEPER = config.containsKey(PARAM_TRANS_LOCKING_ZOOKEEPER) ?
				Integer.parseInt(config.get(PARAM_TRANS_LOCKING_ZOOKEEPER)) : 0;
                if( USE_ZOOKEEPER == 1 ) {
                     // TOOD: exchange this with real Zookeeper Instalation !!!
                    ts = new TestingCluster(3);
                    try {
                        ts.start();
                    } catch (Exception ex) {
                        Logger.getLogger(Listener.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    curator_client = CuratorFrameworkFactory.newClient(Listener.ts.getConnectString(),
                            new ExponentialBackoffRetry(1000,3));
                    curator_client.start();
                    
                    //TODO: where to put ts.stop() and curator_client.stop() ?!
                }
	}
		
	public void contextDestroyed(ServletContextEvent event) {
		if (_crdf != null) {
			try {
				_crdf.close();
			} catch (StoreException e) {
				_log.severe(e.getMessage());
			}
		}
                if( USE_ZOOKEEPER == 1 ) {
                    try {
                        ts.stop();
                    } catch (IOException ex) {
                        Logger.getLogger(Listener.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    curator_client.close();
                }
	}
	
	public static String getFormat(String accept) {
		for (String mimeType : _mimeTypes.keySet()) {
			if (accept.contains(mimeType))
				return _mimeTypes.get(mimeType);
		}
		return null;
	}
	
	public static SerializationFormat getSerializationFormat(String accept) {
		String format = getFormat(accept);
		if (format != null) 
			return _formats.get(format);
		else
			return _formats.get("ntriples");
	}

        public static void changeConsistency(String read_cons, String write_cons) {
            
            if( read_cons.equals("one") )
                read_cons_level = HConsistencyLevel.ONE;
            else if( read_cons.equals("quorum") )
                read_cons_level = HConsistencyLevel.QUORUM;
            else if( read_cons.equals("all") )
                read_cons_level = HConsistencyLevel.ALL;
            else if( read_cons.equals("two") )
                read_cons_level = HConsistencyLevel.TWO;
            else if( read_cons.equals("three") )
                read_cons_level = HConsistencyLevel.THREE;
            else if( read_cons.equals("any") )
                read_cons_level = HConsistencyLevel.ANY;

            if( read_cons.equals("one") )
                write_cons_level = HConsistencyLevel.ONE;
            else if( read_cons.equals("quorum") )
                write_cons_level = HConsistencyLevel.QUORUM;
            else if( read_cons.equals("all") )
                write_cons_level = HConsistencyLevel.ALL;
            else if( read_cons.equals("two") )
                write_cons_level = HConsistencyLevel.TWO;
            else if( read_cons.equals("three") )
                write_cons_level = HConsistencyLevel.THREE;
            else if( read_cons.equals("any") )
                write_cons_level = HConsistencyLevel.ANY;

            Listener.DEFAULT_CONSISTENCY_POLICY = new ConsistencyLevelPolicy() {
			@Override
                        public HConsistencyLevel get(OperationType op_type, String cf) {
                                /*NOTE: based on operation type and/or column family, the
                                   consistency level is tunable
                                   However, we just use for the moment the given parameter
                                */
				if( op_type == OperationType.WRITE )
	                                return Listener.write_cons_level;
				else
					return Listener.read_cons_level;
                        }

                        @Override
                        public HConsistencyLevel get(OperationType op_type) {
				if( op_type == OperationType.WRITE )
	                                return Listener.write_cons_level;
				else
					return Listener.read_cons_level;
                        }   
            };
        }

        public static void changeLockingGranulartiy(String level) {
            ExecuteTransactions.resetLocks();
            if( level.matches("e") ) {
                Listener.DEFAULT_TRANS_LOCKING_GRANULARITY = "1";
            }
            else if( level.matches("ep") ) {
                Listener.DEFAULT_TRANS_LOCKING_GRANULARITY = "2";
            }
            else if( level.matches("epv") ) {
                Listener.DEFAULT_TRANS_LOCKING_GRANULARITY = "3";
            }
        }

        public static void changeReplicationFactor(int factor) {
            Listener.DEFAULT_REPLICATION_FACTOR = factor;
        }

        public static void changeTransactionalSupport(String mode) {
            if( mode.equals("zookeeper") )  {
                // TODO: also initialize curator_client !!!
                Listener.USE_ZOOKEEPER = 1;
            }
            else {
                Listener.USE_ZOOKEEPER = 0;
            }
        }
}
