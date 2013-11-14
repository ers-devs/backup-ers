package edu.kit.aifb.cumulus.store;

import java.nio.ByteBuffer;
import java.io.Writer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import edu.kit.aifb.cumulus.webapp.Listener;
import java.util.Hashtable;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.hector.api.beans.Composite;

public class CassandraRdfHectorFlatHash extends CassandraRdfHectorQuads {
	public static final String CF_S_PO = "SPO";
	private static final String CF_O_SP = "OSP";
	private static final String CF_PO_S = "POS";
	
	private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	public CassandraRdfHectorFlatHash(String hosts) {
		super(hosts);
		_hosts = hosts;
		_cfs.add(CF_S_PO);
		_cfs.add(CF_O_SP);
		_cfs.add(CF_PO_S);
                _maps.put(CF_S_PO, new int[] { 0, 1, 2});
		_maps.put(CF_O_SP, new int[] { 2, 0, 1 });
		_maps.put(CF_PO_S, new int[] { 1, 2, 0 });

                _maps_ext.put(CF_S_PO, new int[] { 0, 1, 2, 3, 4});
		_maps_ext.put(CF_O_SP, new int[] { 2, 0, 1, 3, 4});
		_maps_ext.put(CF_PO_S, new int[] { 1, 2, 0, 3, 4});

                
                 // used to create another triple before loading into different CF for expressing the link
                _maps.put("link", new int[] { 2, 1, 0});
		_maps_br.put(CF_S_PO, new int[] { 0, 1, 2, 3 });
		_maps_br.put(CF_O_SP, new int[] { 2, 0, 1, 3 });
		_maps_br.put(CF_PO_S, new int[] { 1, 2, 0, 3 });
                _maps_br.put("link", new int[] { 2, 1, 0, 3 });
		_maps_br_update_d.put(CF_S_PO, new int[] { 0, 1, 2, 3, 4 });
		_maps_br_update_d.put(CF_O_SP, new int[] { 2, 0, 1, 3, 4 });
		_maps_br_update_d.put(CF_PO_S, new int[] { 1, 2, 0, 3, 4 });
                _maps_br_update_d.put("link", new int[] { 2, 1, 0, 3, 4 });
		_maps_br_update_i.put(CF_S_PO, new int[] { 0, 1, 4, 3, 2 });
		_maps_br_update_i.put(CF_O_SP, new int[] { 4, 0, 1, 3, 2 });
		_maps_br_update_i.put(CF_PO_S, new int[] { 1, 4, 0, 3, 2 });
                _maps_br_update_i.put("link", new int[] { 4, 1, 0, 3, 2 });
	}

        @Override
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitionsVersioning(String keyspace) {
                ColumnFamilyDefinition spo = createCfDefFlatVersioning(CF_S_PO, null, null, ComparatorType.COMPOSITETYPE, keyspace);
                //ColumnFamilyDefinition osp = createCfDefFlatVersioning(CF_O_SP, null, null, ComparatorType.COMPOSITETYPE, keyspace);
		/*ColumnFamilyDefinition pos = createCfDefFlatVersioning(CF_PO_S, Arrays.asList("!p", "!o"), Arrays.asList("!p"),
                        ComparatorType.COMPOSITETYPE, keyspace);*/

		ArrayList<ColumnFamilyDefinition> li = new ArrayList<ColumnFamilyDefinition>();
                // TM : create SPO, Redirects; TODO: do we need these?!
		//li.addAll(super.createColumnFamiliyDefinitions(keyspace));
		li.addAll(Arrays.asList(spo)); //, osp));//, pos));
		return li;
	}

	@Override
	protected List<ColumnFamilyDefinition> createColumnFamiliyDefinitions(String keyspace) {
		ColumnFamilyDefinition spo = createCfDefFlat(CF_S_PO, null, null, ComparatorType.UTF8TYPE, keyspace);
                ColumnFamilyDefinition osp = createCfDefFlat(CF_O_SP, null, null, ComparatorType.UTF8TYPE, keyspace);
                ColumnFamilyDefinition pos = createCfDefFlat(CF_PO_S, Arrays.asList("!p", "!o"), Arrays.asList("!p"),
                        ComparatorType.BYTESTYPE, keyspace);
                ArrayList<ColumnFamilyDefinition> li = new ArrayList<ColumnFamilyDefinition>();
		li.addAll(super.createColumnFamiliyDefinitions(keyspace));
		li.addAll(Arrays.asList(spo, osp, pos));
		return li;
	}

	private ByteBuffer createKey(Node[] nx) {
		ByteBuffer key = ByteBuffer.allocate(2 * 8);
		key.put(Util.hash(nx[0].toN3()));
		key.put(Util.hash(nx[1].toN3()));
		key.flip();
		return key;
	}

        // for all involved entities, get the most recent version and also the lastCommitID
        private boolean fetchMostRecentVersions(String keyspace, String cf, List<Node[]> li,
                String txID, String URN_author,
                Hashtable<String, List<Node[]>> versioned_entities, Hashtable<String, String> previous_commit_id ) {
            // FETCH ALL MOST RECENT VERSIONS FOR ALL ENTITIES and add the new triple
            // get previous versions of all entities involved in this batch
            for (Node[] nx : li) {
                // reorder for the key
                Node[] reordered = Util.reorder(nx, _maps.get(cf));
                String rowKey = new Resource(reordered[0].toString() + "-VER").toN3();
                // if the previous version has been fetched, then add the new prop-value
                if( versioned_entities.contains(rowKey) ) {
                    List<Node[]> all_props = versioned_entities.get(rowKey);
                    all_props.add(nx);
                    versioned_entities.put(rowKey, all_props);
                    continue;
                }
                // else, fetch the old version and add the current one to the list
                String version_key = nx[0].toString();
                String last_ver;
                if( previous_commit_id.contains(rowKey) )
                    last_ver = previous_commit_id.get(rowKey);
                else {
                    /*last_ver = lastVersioningNumber(keyspace, version_key.replaceAll("<", "").
                        replaceAll(">", ""), URN_author);*/
                    last_ver = lastCommitTxID(keyspace, version_key.replaceAll("<", "").
                            replace(">",""));
                    // if last commit ID is greater than current Tx commit id, then abort
                    //as other Tx may have in the meantime
                    if( last_ver.compareToIgnoreCase(txID) > 0 )
                        return false;
                    previous_commit_id.put(rowKey, last_ver);
                }

                Node[] query = new Node[3];
                try {
                    query[0] = getNode(nx[0].toN3(), "s");
                    query[1] = getNode(null, "p");
                    query[2] = getNode(null, "o");
                    // here get the last version for this entity
                    Iterator<Node[]> last_version = queryVersioning(query,
                            Integer.MAX_VALUE, keyspace, 3, last_ver, URN_author);
                    List<Node[]> all_prop_last_v = new ArrayList<Node[]>();
                    while( last_version.hasNext() ) {
                        Node[] ent = last_version.next();
                        ent[0] = new Resource( ent[0].toString().replaceAll("-VER", "") );
                        all_prop_last_v.add(ent);
                    }
                    // add it to the hashtable
                    versioned_entities.put(rowKey, all_prop_last_v);
                } catch (Exception ex) {
                    Logger.getLogger(CassandraRdfHectorFlatHash.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return true;
        }

        // return 1 if no conflicts were encountered when adding the new 'lastCID' 'prevCID' records
        //for all entities; if one such record cannot be added (i.e. a previous tx has already added
        //a lastCID > than whan we add), it must abort and returns -1
        private int commitOrAbort(String keyspace, String txID, String URN_author,
                Hashtable<String, List<Node[]>> versioned_entities,
                Hashtable<String, String> previous_commit_id ) {
            boolean abort = false;
            List<String> committed_entities = new ArrayList<String>();
            // try to commit by inserting  'lastCID' 'prevCID' 
            for( Iterator<String> it=versioned_entities.keySet().iterator(); it.hasNext(); ) {
                String row_entity_key = it.next();
                // there is a list of properties to be added to the new version of this entity
                List<Node[]> entity_old_version = versioned_entities.get(row_entity_key);
                String version_key = ((Node[])entity_old_version.get(0))[0].toString().replaceAll("-VER", "");
                String prev_commit_id = previous_commit_id.get(row_entity_key);

                if( txID == null )
                    // update to next version (increment the previous one)
                    updateToNextVersion(keyspace, version_key.replaceAll("<", "").replaceAll(">", ""),
                        URN_author, Integer.valueOf(prev_commit_id));
                else {
                    // just add the last commit id
                    writeCommitID(keyspace, version_key.replaceAll("<", "").replaceAll(">", ""),
                            URN_author, txID, prev_commit_id);
                    // assuming that this entity was successfully commited
                    committed_entities.add(row_entity_key);
                    if( Listener.CHECK_MY_WRITES == 1 ) {
                        // check if the version we just wrote is the one we read or
                        //is it in the correct tree path
                        if( ! checkMyWrites(keyspace,
                                version_key.replaceAll("<", "").replaceAll(">", ""),
                                txID) ) {
                            abort = true;
                            break;
                        }
                    }
                }
            }
            if( abort && committed_entities.size() > 0 ) {
                for( Iterator<String> it=committed_entities.iterator(); it.hasNext(); ) {
                    String row_entity_key = it.next();
                    List<Node[]> entity_old_version = versioned_entities.get(row_entity_key);
                    String version_key = ((Node[])entity_old_version.get(0))[0].toString().replaceAll("-VER", "");
                    String prev_commit_id = previous_commit_id.get(row_entity_key);

                    // make the prevCID of current txID an 'X' meaning it is not anymore attached
                    //to the version tree
                    abortCommitID(keyspace, version_key.replaceAll("<", "").replaceAll(">", ""),
                            URN_author, txID, prev_commit_id);
                }
                // so it aborted
                return -1;
            }
            //so it commited
            return 0;
        }

        @Override
	protected int batchInsertVersioning(String cf, List<Node[]> li, String keyspace,
                String URN_author, String txID) {
                Hashtable<String, List<Node[]>> versioned_entities = new Hashtable<String, List<Node[]>>();
                Hashtable<String, String> previous_commit_id  = new Hashtable<String, String>();
                boolean successful_fetch = fetchMostRecentVersions(keyspace, cf, li, txID,
                        URN_author, versioned_entities, previous_commit_id);
                if( ! successful_fetch ) {
                    // it means that for one entity there is already a lastCID > txID
                    // we have to abort here the insertion
                    return 2;
                }

                /*
// SIMULATE SOME DELAY HERE !!
try{
Thread.sleep(System.currentTimeMillis()%10000);
}catch(Exception ex) {}
// END [THIS MUST BE ERASED IN PRODUCTION!!]*/

                // add the new triples to previous versions
                for(Iterator<Node[]> it_triples=li.iterator(); it_triples.hasNext(); ) {
                    Node[] triple = it_triples.next();
                    String rowKey = new Resource(triple[0].toString() + "-VER").toN3();

                    List<Node[]> prev_version = versioned_entities.get(rowKey);
                    prev_version.add(triple);
                    versioned_entities.put(rowKey, prev_version);
                }

                //SPO
                // insert 's-VER' and 's-URN' new versions
                Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
                for( Iterator<String> it=versioned_entities.keySet().iterator(); it.hasNext(); ) {
                    String row_entity_key = it.next();
                    // there is a list of properties to be added to the new version of this entity
                    List<Node[]> entity_old_version = versioned_entities.get(row_entity_key);
                    String old_version_num = previous_commit_id.get(row_entity_key);

                    for(Iterator it_old_v=entity_old_version.iterator(); it_old_v.hasNext(); ) {
                        Node[] nx = (Node[]) it_old_v.next();
                        // reorder for the key
                        Node[] reordered = Util.reorder(nx, _maps.get(cf));
                        String rowKey = new Resource(reordered[0].toString()).toN3();
                        if( !reordered[0].toString().contains("-VER") )
                            rowKey = new Resource(reordered[0].toString() + "-VER").toN3();

                        /* this is the approach without using Snowflake
                        int next_ver = old_version_num+1;*/
                        String next_ver;
                        // if txID is different than null, it means this is called in a transactional context
                        if( txID != null ) 
                            next_ver = txID;
                        else
                            next_ver = String.valueOf(Integer.valueOf(old_version_num)+1);

                        // VER, URN
                        Composite colKey = new Composite();
                        colKey.addComponent(next_ver, StringSerializer.get());
                        colKey.addComponent(URN_author, StringSerializer.get());
                        String colKey_s = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
                        colKey.addComponent(colKey_s, StringSerializer.get());
                        HColumn<Composite, String> hColumnObj_itemID = HFactory.createColumn(colKey, "",
                                new CompositeSerializer(),
                                StringSerializer.get());
                        m.addInsertion(rowKey, cf, hColumnObj_itemID);

                        // URN, VER
                        rowKey = new Resource(reordered[0].toString() + "-URN").toN3();
                        colKey = new Composite();
                        colKey.addComponent(URN_author, StringSerializer.get());
                        colKey.addComponent(String.valueOf(next_ver), StringSerializer.get());
                        colKey_s = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
                        colKey.addComponent(colKey_s, StringSerializer.get());

                        hColumnObj_itemID =HFactory.createColumn(colKey, "",
                                new CompositeSerializer(),
                                StringSerializer.get());
                        m.addInsertion(rowKey, cf, hColumnObj_itemID);
                    }
                    m.execute();
                }
                // now try to write all CID,prevCID; if check my writes is enabled, it can abort in case
                //there are conflicts
                return commitOrAbort(keyspace, txID, URN_author, versioned_entities, previous_commit_id);
	}

        @Override
	public int batchUpdateVersioning(String cf, List<Node[]> li, String keyspace,
               String URN_author, String txID) {
            Hashtable<String, List<Node[]>> versioned_entities = new Hashtable<String, List<Node[]>>();
            Hashtable<String, String> previous_commit_id  = new Hashtable<String, String>();
            boolean successful_fetch = fetchMostRecentVersions(keyspace, cf, li, txID,
                    URN_author, versioned_entities, previous_commit_id);
            if( ! successful_fetch ) {
                // it means that for one entity there is already a lastCID > txID
                // we have to abort here the insertion
                return 2;
            }

            // now update the properties into the recent versions fetched
            for( Iterator<Node[]> it=li.iterator(); it.hasNext(); ) {
                Node[] current = it.next();
                String prop = current[1].toN3();
                String value_old = current[2].toN3();
                Node value_new = current[3];

                List<Node[]> entity_version = versioned_entities.get("<"+current[0].toString()+"-VER>");
                for( Iterator<Node[]> it_version=entity_version.iterator(); it_version.hasNext(); ) {
                    Node[] ver_ent = it_version.next();
                    String ver_prop = ver_ent[1].toN3();
                    String ver_value = ver_ent[2].toN3();
                    // check if this is the triple to be replaces
                    if( prop.equals(ver_prop) && value_old.equals(ver_value) )
                        ver_ent[2] = value_new;
                }
            }

            //SPO
            // insert 's-VER' and 's-URN' new versions
            Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
            for( Iterator<String> it=versioned_entities.keySet().iterator(); it.hasNext(); ) {
                String row_entity_key = it.next();
                // there is a list of properties to be added to the new version of this entity
                List<Node[]> entity_old_version = versioned_entities.get(row_entity_key);
                String old_version_num = previous_commit_id.get(row_entity_key);

                for(Iterator it_old_v=entity_old_version.iterator(); it_old_v.hasNext(); ) {
                    Node[] nx = (Node[]) it_old_v.next();
                    // reorder for the key
                    Node[] reordered = Util.reorder(nx, _maps.get(cf));
                    String rowKey = new Resource(reordered[0].toString()).toN3();
                    if( !reordered[0].toString().contains("-VER") )
                        rowKey = new Resource(reordered[0].toString() + "-VER").toN3();

                    /* this is the approach without using Snowflake
                    int next_ver = old_version_num+1;*/
                    String next_ver;
                    // if txID is different than null, it means this is called in a transactional context
                    if( txID != null )
                        next_ver = txID;
                    else
                        next_ver = String.valueOf(Integer.valueOf(old_version_num)+1);

                    // VER, URN
                    Composite colKey = new Composite();
                    colKey.addComponent(next_ver, StringSerializer.get());
                    colKey.addComponent(URN_author, StringSerializer.get());
                    String colKey_s = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
                    colKey.addComponent(colKey_s, StringSerializer.get());
                    HColumn<Composite, String> hColumnObj_itemID = HFactory.createColumn(colKey, "",
                            new CompositeSerializer(),
                            StringSerializer.get());
                    m.addInsertion(rowKey, cf, hColumnObj_itemID);

                    // URN, VER
                    rowKey = new Resource(reordered[0].toString() + "-URN").toN3();
                    colKey = new Composite();
                    colKey.addComponent(URN_author, StringSerializer.get());
                    colKey.addComponent(String.valueOf(next_ver), StringSerializer.get());
                    colKey_s = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
                    colKey.addComponent(colKey_s, StringSerializer.get());

                    hColumnObj_itemID =HFactory.createColumn(colKey, "",
                            new CompositeSerializer(),
                            StringSerializer.get());
                    m.addInsertion(rowKey, cf, hColumnObj_itemID);
                }
                m.execute();
            }
            // now try to write all CID,prevCID; if check my writes is enabled, it can abort in case
            //there are conflicts
            return commitOrAbort(keyspace, txID, URN_author, versioned_entities, previous_commit_id);
	}

        @Override
	protected void batchInsert(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {
                        super.batchInsert(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));

				ByteBuffer rowKey = createKey(new Node[] { reordered[0], reordered[1] });
				String colKey = reordered[2].toN3();
				m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
				m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
				m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));
			}
			m.execute();
		}
		else {
                        Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				String rowKey = reordered[0].toN3();

                                // for ERS_bridges_.. keyspaces, use the column value; it is easier for querying afterwards
                                // few keyspacs such as ERS_graphs cannot be decoded
                                String keyspace_decoded = this.decodeKeyspace(keyspace);
                                if( keyspace_decoded != null && keyspace_decoded.startsWith(Listener.BRIDGES_KEYSPACE+"_") ) {
                                    String colKey = reordered[1].toN3();
                                    m.addInsertion(rowKey, cf, HFactory.createStringColumn(colKey, reordered[2].toN3()));
                                }
                                else {
                                    String colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
                                    m.addInsertion(rowKey, cf, HFactory.createStringColumn(colKey, ""));
                                }
    			}
                        m.execute();
		}
	}
	
	// TM
	@Override
	protected void batchDelete(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {
			super.batchDelete(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				ByteBuffer rowKey = createKey(new Node[] { reordered[0], reordered[1] });
				String colKey = reordered[2].toN3();
				// delete the full row 
				m.addDeletion(rowKey.array(), cf);
			}
			m.execute();
		}
		else {
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				String rowKey = reordered[0].toN3();
				String colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
				m.addDeletion(rowKey, cf, colKey, _ss);
			}
			m.execute();
		}
	}

	// TM
	@Override
	protected void batchDeleteRow(String cf, List<Node[]> li, String keyspace) {
		if (cf.equals(CF_C_SPO)) {
			super.batchDeleteRow(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				ByteBuffer rowKey = createKey(new Node[] { reordered[0], reordered[1] });
				String colKey = reordered[2].toN3();
				// delete the full row 
				m.addDeletion(rowKey.array(), cf);
			}
			m.execute();
		}
		else {
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				// reorder for the key
				Node[] reordered = Util.reorder(nx, _maps.get(cf));
				String rowKey = reordered[0].toN3();
				m.addDeletion(rowKey, cf);
_log.info("Delete full row for " + rowKey + " cf= " + cf);
			}
			m.execute();
		}
	}

	// create a batch of different operations and execute them all together
	@Override
	protected void batchRun(String cf, List<Node[]> li, String keyspace) { 
		if (cf.equals(CF_C_SPO)) {
			// this is not implemented, we don't care about CSPO CF 
			super.batchRun(cf, li, keyspace);
		}
		else if (cf.equals(CF_PO_S)) {
			Mutator<byte[]> m = HFactory.createMutator(getExistingKeyspace(keyspace), _bs);
			for (Node[] nx : li) {
				Node[] reordered;
				ByteBuffer rowKey;
				String colKey;

//				_log.info("$$$ " + nx[0].toString() + " " + nx[1].toString() + " " + nx[2].toString() + " " + nx[3].toString());

				switch( Integer.parseInt(nx[3].toString()) ) { 
					case 0:
						//ignore query
						continue; 
					case 1: 
						//insertion
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));
						break;
                    case 11:
						// insertion link
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));

                        // add the back link as well
                        reordered = Util.reorder(nx, _maps_br.get("link"));
                        rowKey = createKey(new Node[] { reordered[0], reordered[1] });
                        colKey = reordered[2].toN3();
                        m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
                        m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
                        m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));
                        break;
					case 2: 
						//deletion 
	 					// reorder for the key
						reordered = Util.reorder(nx, _maps_br.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						// delete the full row 
						m.addDeletion(rowKey.array(), cf);
						break;
                    case 21:
						//deletion link
	 					// reorder for the key
						reordered = Util.reorder(nx, _maps_br.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						// delete the full row
						m.addDeletion(rowKey.array(), cf);

                        // delete the back link as well
                        if( nx[2] instanceof Resource ) {
                            reordered = Util.reorder(nx, _maps_br.get("link") );
                            rowKey = createKey(new Node[] { reordered[0], reordered[1] });
                            colKey = reordered[2].toN3();
                            // delete the full row containing the back link
                            m.addDeletion(rowKey.array(), cf);
                        }
						break;
					case 3: 
						//update, run a delete and an insert 
						//deletion 
	 					// reorder for the key
						reordered = Util.reorder(nx, _maps_br_update_d.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						// delete the full row 
						m.addDeletion(rowKey.array(), cf);

						//insertion
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br_update_i.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));
						break;
                     case 4: 
                        // delete the full entity
                        Node[] query = new Node[3];
                        query[0] = nx[0];
                        query[1] = new Variable("p");
                        query[2] = new Variable("o");
                        try {
                            Iterator<Node[]> it = this.query(query, Integer.MAX_VALUE, keyspace);
                            // if data, delete :) 
                            for( ; it.hasNext(); ) {
                                Node[] n = (Node[]) it.next();
                                //this.deleteData(n, keyspace, 0);
						        rowKey = createKey(new Node[] { n[0], n[1] });
                                m.addDeletion(rowKey.array(), cf);
                            }
                        } catch (StoreException ex) {
                            _log.severe(ex.getMessage());
                        }
                        break;
                     case 31:
						//update with link, run a delete and an insert
						//deletion
	 					// reorder for the key
						reordered = Util.reorder(nx, _maps_br_update_d.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						// delete the full row
						m.addDeletion(rowKey.array(), cf);

                        // delete the back link as well
                        if( nx[2] instanceof Resource ) {
                            reordered = Util.reorder(nx, _maps_br_update_d.get("link"));
                            rowKey = createKey(new Node[] { reordered[0], reordered[1] });
                            colKey = reordered[2].toN3();
                            // delete the full row containing the back link
                            m.addDeletion(rowKey.array(), cf);
                        }

						//insertion
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br_update_i.get(cf));
						rowKey = createKey(new Node[] { reordered[0], reordered[1] });
						colKey = reordered[2].toN3();
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
						m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));

                        // insert also the new back link
                        if( nx[5] instanceof Resource ) {
                            reordered = Util.reorder(nx, _maps_br_update_i.get("link"));
                            rowKey = createKey(new Node[] { reordered[0], reordered[1] });
                            colKey = reordered[2].toN3();
                            m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn(colKey, ""));
                            m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!p", reordered[0].toN3()));
                            m.addInsertion(rowKey.array(), cf, HFactory.createStringColumn("!o", reordered[1].toN3()));
                        }
						break;
					default:	
						_log.info("OPERATION UNKNOWN, moving to next quad");
						break;
				}
			}
			m.execute();
		}
		else {
			Mutator<String> m = HFactory.createMutator(getExistingKeyspace(keyspace), _ss);
			for (Node[] nx : li) {
				Node[] reordered;
				String rowKey;
				String colKey;

//				_log.info("$$$ " + nx[0].toString() + " " + nx[1].toString() + " " + nx[2].toString() + " " + nx[3].toString());

				switch( Integer.parseInt(nx[3].toString()) ) { 
					case 0:
						//ignore query
						continue; 
					case 1: 
						// insertion 
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br.get(cf));
						rowKey = reordered[0].toN3();
						colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
						m.addInsertion(rowKey, cf, HFactory.createStringColumn(colKey, ""));
						break;
					case 2: 
						// deletion
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br.get(cf));
						rowKey = reordered[0].toN3();
						colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
						m.addDeletion(rowKey, cf, colKey, _ss);
						break;
					case 3: 
						// update = delete+insert 
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br_update_d.get(cf));
						rowKey = reordered[0].toN3();
						colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
						m.addDeletion(rowKey, cf, colKey, _ss);
						// reorder for the key
						reordered = Util.reorder(nx, _maps_br_update_i.get(cf));
						rowKey = reordered[0].toN3();
						colKey = Nodes.toN3(new Node[] { reordered[1], reordered[2] });
						m.addInsertion(rowKey, cf, HFactory.createStringColumn(colKey, ""));
						break;
                    case 4: 
                         // delete the full entity
                        Node[] query = new Node[3];
                        query[0] = nx[0];
                        query[1] = new Variable("p");
                        query[2] = new Variable("o");
                        try {
                            Iterator<Node[]> it = this.query(query, Integer.MAX_VALUE, keyspace);
                            // if data, delete :) 
                            for( ; it.hasNext(); ) {
                                Node[] n = (Node[]) it.next();
                                //this.deleteData(n, keyspace, 0);
						        rowKey = n[0].toN3();
                                m.addDeletion(rowKey, cf);
                            }
                        } catch (StoreException ex) {
                            _log.severe(ex.getMessage());
                        }
                        break;
					default:
						_log.info("OPERATION UNKNOWN, moving to next quad");
						break;
				}
			}
			m.execute();
		}
	}

	private String selectColumnFamily(Node[] q) {
		if (!isVariable(q[0])) {
			if (!isVariable(q[2]))
				return CF_O_SP; // SO
			else
				return CF_S_PO; // S, SP
		}
		
		// here S is variable
		if (!isVariable(q[2])) {
			if (!isVariable(q[1]))
				return CF_PO_S; // PO
			else
				return CF_O_SP; // O
		}
		
		// here S,O are variables
		
		if (!isVariable(q[1])) 
			return CF_PO_S; // P
		
		// for pattern with no constants, use SPO by default
		return CF_S_PO;
	}

        private String selectColumnFamilyVersioning(Node[] q) {
            // the only existing column family is SPO
            return CF_S_PO;
	}

        /* situation options:
         *  1. ID:*:*
         *  2. URN:*:*
         *  3. ID:URN:*
         *  4. URN:ID:*
         */
        @Override
	public Iterator<Node[]> queryVersioning(Node[] query, int limit, String keyspace, 
                int situation, String ID, String URN) throws StoreException {
		Iterator<Node[]> it = super.query(query, limit, keyspace);
		if (it != null) {
			return it;
		}
		String columnFamily = CF_S_PO;
		int[] map = _maps.get(columnFamily);
		Node[] q = Util.reorder(query, map);
//		_log.info("query: " + Nodes.toN3(query) + " idx: " + columnFamily + " reordered: " + Nodes.toN3(q));
		if (isVariable(q[0])) {
			// scan over all
			throw new UnsupportedOperationException("triple patterns must have at least one constant");
		}
		else {
                       
                    // SPO cf has one node as key and two nodes as colname
                    Node[] nxKey = getQueryKeyBySituation(q[0], situation);
                    String key_ID = nxKey[0].toN3();
                    int colNameTupleLength = 2;
                    // check ID and get the latest if neccessary
                    if( ID != null && ID.contains("last") && URN != null )
                        /*ID = String.valueOf(lastVersioningNumber(keyspace,q[0].toString()
                                , URN));*/
                        ID = lastCommitTxID(keyspace, q[0].toString());

                      /* TM: query composite */
                    SliceQuery<String, Composite, String> query_comp = HFactory.createSliceQuery(getExistingKeyspace(keyspace),
                             StringSerializer.get(), CompositeSerializer.get(), StringSerializer.get());
                    Composite columnStart = getCompositeStart(ID, URN, situation, q);
                    Composite columnEnd = getCompositeEnd(ID, URN, situation, q);
                    query_comp.setColumnFamily(columnFamily).setKey(key_ID);

        /*_log.info("QUERY COMPOSITE ...on CF " + columnFamily + "..... on key  " + key_ID);
        _log.info("QUERY COMPOSITE " + Nodes.toN3(q) + "--" + Nodes.toN3(new Node[] {q[1], q[2]})) ;
        _log.info("QUERY COMPOSITE situation: " + situation );*/

                    // use the extended map
                    map = _maps_ext.get(columnFamily);
                    it = new ColumnSliceIteratorComposite<String>(query_comp, nxKey,
                            columnStart, columnEnd, map, limit, colNameTupleLength, query);
                        }
		return it;
	}

        private Composite getCompositeStart(String ID, String URN, int situation,
                Node[] q) {
            Composite columnStart = new Composite();
            switch( situation ) {
                    // ID:*:*
                case 1:
                    columnStart.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    break;
                case 2:
                    // URN:*:*
                    columnStart.addComponent(0, URN, Composite.ComponentEquality.EQUAL);
                    break;
                case 3:
                    // ID:URN:*
                    columnStart.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    columnStart.addComponent(1, URN, Composite.ComponentEquality.EQUAL);
                    break;
                case 4:
                    // URN:ID:*
                    columnStart.addComponent(0, URN, Composite.ComponentEquality.EQUAL);
                    columnStart.addComponent(1, ID, Composite.ComponentEquality.EQUAL);
                    break;
                case 5:
                    // *:*:*
                    break;
                case 6:
                    // ID:URN:prop
                    columnStart.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    columnStart.addComponent(1, URN, Composite.ComponentEquality.EQUAL);
                    columnStart.addComponent(2, q[1].toN3(), Composite.ComponentEquality.EQUAL);
                    break;
                 case 7:
                    // ID:URN:prop-value
                    columnStart.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    columnStart.addComponent(1, URN, Composite.ComponentEquality.EQUAL);
                    columnStart.addComponent(2, q[1].toN3() + " " + q[2].toN3(), Composite.ComponentEquality.EQUAL);
                    break;
                default:
                    break;
            }
            return columnStart;
        }

        private Composite getCompositeEnd(String ID, String URN, int situation,
                Node[] q) {
            Composite columnStop = new Composite();
            switch( situation ) {
                    // ID:*:*
                case 1:
                    columnStop.addComponent(0, ID, Composite.ComponentEquality.GREATER_THAN_EQUAL);
                    break;
                case 2:
                    // URN:*:*
                    columnStop.addComponent(0, URN, Composite.ComponentEquality.GREATER_THAN_EQUAL);
                    break;
                case 3:
                    // ID:URN:*
                    columnStop.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    columnStop.addComponent(1, URN, Composite.ComponentEquality.GREATER_THAN_EQUAL);
                    break;
                case 4:
                    // URN:ID:*
                    columnStop.addComponent(0, URN, Composite.ComponentEquality.EQUAL);
                    columnStop.addComponent(1, ID, Composite.ComponentEquality.GREATER_THAN_EQUAL);
                    break;
                case 5:
                    // *:*:*
                    break;
                 case 6:
                    // ID:URN:prop
                    columnStop.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    columnStop.addComponent(1, URN, Composite.ComponentEquality.EQUAL);
                    columnStop.addComponent(2, q[1].toN3()+"_", Composite.ComponentEquality.GREATER_THAN_EQUAL);
                    break;
                 case 7:
                    // ID:URN:prop-value
                    columnStop.addComponent(0, ID, Composite.ComponentEquality.EQUAL);
                    columnStop.addComponent(1, URN, Composite.ComponentEquality.EQUAL);
                    columnStop.addComponent(2, q[1].toN3() + " " + q[2].toN3() + "_", Composite.ComponentEquality.GREATER_THAN_EQUAL);
                    break;
                default:
                    break;
            }
            return columnStop;
        }

        private Node[] getQueryKeyBySituation(Node entity, int situation) {
             Resource ent;
             switch( situation ) {
                    // ID:*:*
                case 1:
                    // ID:URN:*
                case 3:
                    // *:*:*
                case 5:
                    // ID:URN:prop
                case 6:
                    // ID:URN:prop-value
                case 7:
                    return new Node[] { new Resource(entity.toString()+"-VER")};
                case 2:
                    // URN:*:*
                case 4:
                    // URN:ID:*
                    return new Node[] { new Resource(entity.toString()+"-URN")};
                default:
                    return null;
            }
        }

        @Override
	public Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException {
		Iterator<Node[]> it = super.query(query, limit, keyspace);
		if (it != null) {
			return it;
		}
		String columnFamily = selectColumnFamily(query);
		int[] map = _maps.get(columnFamily);
		Node[] q = Util.reorder(query, map);
//		_log.info("query: " + Nodes.toN3(query) + " idx: " + columnFamily + " reordered: " + Nodes.toN3(q));

		if (isVariable(q[0])) {
			// scan over all
			throw new UnsupportedOperationException("triple patterns must have at least one constant");
		}
		else {
			if (columnFamily.equals(CF_PO_S)) {
                                if (isVariable(q[1])) {
					// we use a secondary index for P only when no other constant is given
					IndexedSlicesQuery<byte[],String,String> isq = HFactory.createIndexedSlicesQuery(getExistingKeyspace(keyspace), _bs, _ss, _ss)
						.setColumnFamily(columnFamily)
						.addEqualsExpression("!p", q[0].toN3())
						.setReturnKeysOnly();
					it = new HashIndexedSlicesQueryIterator(isq, map, limit, columnFamily, getExistingKeyspace(keyspace));
				}
				else {
					// here we always have a PO lookup, POS (=SPO) is handled by OSP or SPO
					// we retrieve all columns from a single row
					// in POS the keys are hashes, we retrieve P and O from columns !p and !o
					ByteBuffer key = createKey(new Node[] { q[0], q[1] });

					SliceQuery<byte[],String,String> sq = HFactory.createSliceQuery(getExistingKeyspace(keyspace), _bs, _ss, _ss)
						.setColumnFamily(columnFamily)
						.setKey(key.array())
						.setRange("!o", "!p", false, 2);
					QueryResult<ColumnSlice<String,String>> res = sq.execute();

					if (res.get().getColumns().size() == 0)
						return new ArrayList<Node[]>().iterator();

					Node[] nxKey = new Node[2];
					try {
						nxKey[0] = NxParser.parseNode(res.get().getColumnByName("!p").getValue());
						nxKey[1] = NxParser.parseNode(res.get().getColumnByName("!o").getValue());
					}
					catch (ParseException e) {
						e.printStackTrace();
					}

					it = new ColumnSliceIterator<byte[]>(sq, nxKey, "<", "", map, limit, 1);
				}
			}
			else {
				String startRange = "", endRange = "";
				if (!isVariable(q[1])) {
					// if there is more than one constant, we need to modify the range
					startRange = q[1].toN3();
					endRange = startRange + "_";

					if (!isVariable(q[2])) {
						startRange = Nodes.toN3(new Node[] { q[1], q[2] });
						endRange = startRange;
					}
				}

				// SPO, OSP cfs have one node as key and two nodes as colname
				Node[] nxKey = new Node[] { q[0] };
				String key = q[0].toN3();
				int colNameTupleLength = 2;

				SliceQuery<String,String,String> sq = HFactory.createSliceQuery(
                                        getExistingKeyspace(keyspace), _ss, _ss, _ss)
					.setColumnFamily(columnFamily)
					.setKey(key);

				it = new ColumnSliceIterator<String>(sq, nxKey,
                                        startRange, endRange, map, limit, colNameTupleLength);
                        }

		}
		return it;
	}

        @Override
	public Iterator<HColumn<String,String>> queryBrigesTimeStats(Node[] query, String keyspace,
                String start_time, String end_time) throws StoreException {
             
                // SPO, OSP cfs have one node as key and two nodes as colname
                Node[] nxKey = new Node[] { query[0] };
                String key = query[0].toN3();
                int colNameTupleLength = 1;

                SliceQuery<String,String,String> sq = HFactory.createSliceQuery(
                        getExistingKeyspace(keyspace), _ss, _ss, _ss)
                        .setColumnFamily(CF_S_PO)
                        .setRange(start_time, end_time, false, Integer.MAX_VALUE)
                        .setKey(key);
                QueryResult<ColumnSlice<String,String>> results = sq.execute();
                List<HColumn<String,String>> columns = results.get().getColumns();
                Iterator<HColumn<String,String>> it_columns = columns.iterator();
		return it_columns;
	}

	//TM: added for supporting (?,?,?,g) queries
	//NOTE: pass the actual name of the keyspace (hashed value in case of graphs) 
	public int queryEntireKeyspace(String keyspace, Writer out, int limit) throws IOException { 
		int row_count = ( limit > 100) ? 100 : limit;
		int total_row_count=0;

		String decoded_keyspace = this.decodeKeyspace(keyspace);
		// String(row), String(column_name), String(column_value)
		Keyspace k = getExistingKeyspace(keyspace); 
        	RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
	            .createRangeSlicesQuery(k, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
        	    .setColumnFamily("SPO")
		    .setRange(null, null, false, Integer.MAX_VALUE)
	            .setRowCount((row_count==1)?2:row_count);		//do this trick because if row_count=1 and only one "null" record, then stucks in a loop
         	String last_key = null;

	        while (true) {
        	    rangeSlicesQuery.setKeys(last_key, null);
	  	    try {
        	    	    QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		            OrderedRows<String, String, String> rows = result.get();
		            Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

	        	    // we'll skip this first one, since it is the same as the last one from previous time we executed
		            if (last_key != null && rowsIterator != null) 
				    rowsIterator.next();   

			    while (rowsIterator.hasNext()) {
				    Row<String, String, String> row = rowsIterator.next();
				    last_key = row.getKey();
			 	    // print even if we just have row_key but none columns ?! 
				    if (row.getColumnSlice().getColumns().isEmpty()) {
				    	/*StringBuffer buf = new StringBuffer(); 
		   		        buf.append(row.getKey()); 
					buf.append(" NULL NULL . "); 
				        buf.append(keyspace + "\n");
  				    	out.println(buf.toString());
					++total_row_count; 
				        if( total_row_count >= limit ) 
						return total_row_count;*/
					continue;
				    }
				    for(Iterator it = row.getColumnSlice().getColumns().iterator(); it.hasNext(); ) { 		
				        HColumn c = (HColumn)it.next(); 
				    	StringBuffer buf = new StringBuffer(); 
		   		        buf.append(row.getKey()); 
					buf.append(" "); 
					buf.append(c.getName()); 
				        buf.append(" "); 
					buf.append(c.getValue());
				        buf.append(decoded_keyspace + "\n");
  				    	out.write(buf.toString());
					// because the same row key can contain multiple column records, we consider a row each of them rather than the entire row :) 
					// (see how cumulusrdf flat storage works)
					++total_row_count; 
				        if( total_row_count >= limit ) 
						return total_row_count;
				    }
			     }
	            	     if (rows.getCount() < row_count)
        	             	break;
			} catch(Exception e){ 
				e.printStackTrace(); 
				out.write("Exception message: " + e.getMessage() + "\n");
				out.write("Exception: " + e.toString());
				return -1;
			}
        	}
		return total_row_count;
	}
	
	// iterate the keyspaces and query each one of them 
	public int queryAllKeyspaces(int limit, Writer out) throws IOException { 
		int total_rows = 0;
		for (KeyspaceDefinition ksDef : _cluster.describeKeyspaces()) {
			String keyspace = ksDef.getName();
			// only keyspaces that uses as prefix our pre-defined one
			if( ! keyspace.startsWith(Listener.DEFAULT_ERS_KEYSPACES_PREFIX) ||
			      keyspace.equals(Listener.GRAPHS_NAMES_KEYSPACE) ) 
				continue;
			// query this keyspace
			total_rows += queryEntireKeyspace(keyspace, out, limit);
		}
		return total_rows;
	}


        public List<String> queryAllRowKeys(String keyspace, int limit) {
		int row_count = ( limit > 100) ? 100 : limit;
                List<String> results = new ArrayList<String>();

		String decoded_keyspace = this.decodeKeyspace(keyspace);
		// String(row), String(column_name), String(column_value)
		Keyspace k = getExistingKeyspace(keyspace);
        	RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
	            .createRangeSlicesQuery(k, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
        	    .setColumnFamily("SPO")
		    .setRange(null, null, false, row_count)
	            .setRowCount((row_count==1)?2:row_count);		//do this trick because if row_count=1 and only one "null" record, then stucks in a loop
         	String last_key = null;

	        while (true) {
        	    rangeSlicesQuery.setKeys(last_key, null);
                    QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
                    OrderedRows<String, String, String> rows = result.get();
                    Iterator<Row<String, String, String>> rowsIterator = rows.iterator();

                    // we'll skip this first one, since it is the same as the last one from previous time we executed
                    if (last_key != null && rowsIterator != null)
                            rowsIterator.next();

                    while (rowsIterator.hasNext()) {
                            Row<String, String, String> row = rowsIterator.next();
                            last_key = row.getKey();
                            results.add(row.getKey());
                    }
                    if (rows.getCount() < row_count)
                        break;
        	}
		return results;
	}
}
