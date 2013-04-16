package edu.kit.aifb.cumulus.webapp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.yaml.snakeyaml.Yaml;

import edu.kit.aifb.cumulus.store.CassandraRdfHectorHierHash;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorQuads;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;

import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.cassandra.service.OperationType;

import edu.kit.aifb.cumulus.store.sesame.SPARQLResultsNxWriterFactory;
import edu.kit.aifb.cumulus.webapp.formatter.HTMLFormat;
import edu.kit.aifb.cumulus.webapp.formatter.NTriplesFormat;
import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import edu.kit.aifb.cumulus.webapp.formatter.StaxRDFXMLFormat;

//import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
/** 
 * 
 * @author aharth
 */
public class Listener implements ServletContextListener {

	private static final String PARAM_CONFIGFILE = "config-file";
	
	private static final String PARAM_HOSTS = "cassandra-hosts";
	private static final String PARAM_KEYSPACE = "cassandra-keyspace";
	private static final String PARAM_LAYOUT = "storage-layout";
	private static final String PARAM_PROXY_MODE = "proxy-mode";
//	private static final String PARAM_RESOURCE_PREFIX = "resource-prefix";
//	private static final String PARAM_DATA_PREFIX = "data-prefix";
	private static final String PARAM_TRIPLES_SUBJECT = "triples-subject";
	private static final String PARAM_TRIPLES_OBJECT = "triples-object";
	private static final String PARAM_QUERY_LIMIT = "query-limit";
	private static final String PARAM_TUPLE_LENGTH = "tuple_length";
	
	private static final String[] CONFIG_PARAMS = new String[] {
		PARAM_HOSTS, PARAM_KEYSPACE, PARAM_LAYOUT, PARAM_PROXY_MODE,
		//PARAM_RESOURCE_PREFIX, PARAM_DATA_PREFIX,
		PARAM_TRIPLES_OBJECT,
		PARAM_TRIPLES_SUBJECT, PARAM_QUERY_LIMIT
		};
	
//	private static final String DEFAULT_RESOURCE_PREFIX = "resource";
//	private static final String DEFAULT_DATA_PREFIX = "data";
	private static final int DEFAULT_TRIPLES_SUBJECT = -1;
	private static final int DEFAULT_TRIPLES_OBJECT = 5000;
	private static final int DEFAULT_QUERY_LIMIT = -1;
	
	private static final String LAYOUT_SUPER = "super";
	private static final String LAYOUT_FLAT = "flat";
	public static final String AUTHOR_KEYSPACE = "ERS_authors";

	// NOTE: consistency level is tunable per keyspace, per CF, per operation type 
        // for the moment all keyspaces use this default policy 
	public static final ConsistencyLevelPolicy DEFAULT_CONSISTENCY_POLICY = new ConsistencyLevelPolicy() { 
			@Override
                        public HConsistencyLevel get(OperationType op_type, String cf) {
                                /*NOTE: based on operation type and/or column family, the 
                                   consistency level is tunable
                                   However, we just use for the moment the given parameter 
                                */
				if( op_type == OperationType.WRITE ) 
	                                return HConsistencyLevel.ALL;
				else 
					return HConsistencyLevel.ONE;
                        }   
                                                                                                       
                        @Override
                        public HConsistencyLevel get(OperationType op_type) {
				if( op_type == OperationType.WRITE ) 
	                                return HConsistencyLevel.ALL;
				else
					return HConsistencyLevel.ONE;
                        }   
	};
	// NOTE: this can be adjusted per keyspace, the default one is used for now by all of the keyspaces
	public static final Integer DEFAULT_REPLICATION_FACTOR = 3; 

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
				String value = ctx.getInitParameter(param);
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
		
//		if (!config.containsKey(PARAM_HOSTS) || !config.containsKey(PARAM_KEYSPACE) ||
		if (!config.containsKey(PARAM_HOSTS) || !config.containsKey(PARAM_LAYOUT)) {
			_log.severe("config must contain at least these parameters: " + (Arrays.asList(PARAM_HOSTS, PARAM_KEYSPACE, PARAM_LAYOUT)));
			ctx.setAttribute(ERROR, "params missing");
			return;
		}
		try {
			String hosts = config.get(PARAM_HOSTS);
			String keyspace = config.get(PARAM_KEYSPACE);
			String layout = config.get(PARAM_LAYOUT);
			
			_log.info("hosts: " + hosts);
			_log.info("keyspace: " + keyspace);
			_log.info("storage layout: " + layout);

			// start embedded Cassandra
			//EmbeddedCassandraServerHelper.startEmbeddedCassandra();
			
			if (LAYOUT_SUPER.equals(layout))
				_crdf = new CassandraRdfHectorHierHash(hosts);
			else if (LAYOUT_FLAT.equals(layout))
				_crdf = new CassandraRdfHectorFlatHash(hosts);
			else
				throw new IllegalArgumentException("unknown storage layout");
			// set some cluster wide parameters 
			_crdf.open();
   			// create the Authors keyspace
			_crdf.createKeyspace(AUTHOR_KEYSPACE);
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

		ctx.setAttribute(TRIPLES_SUBJECT, subjects);
		ctx.setAttribute(TRIPLES_OBJECT, objects);
		ctx.setAttribute(QUERY_LIMIT, queryLimit);
		
		if (config.containsKey(PARAM_PROXY_MODE)) {
			boolean proxy = Boolean.parseBoolean(config.get(PARAM_PROXY_MODE));
			if (proxy)
				ctx.setAttribute(PROXY_MODE, true);
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
}
