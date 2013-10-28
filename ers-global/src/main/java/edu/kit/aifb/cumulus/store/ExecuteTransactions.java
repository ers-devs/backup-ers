package edu.kit.aifb.cumulus.store;

import org.semanticweb.yars.nx.Node;
import edu.kit.aifb.cumulus.webapp.Listener;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.semanticweb.yars.nx.parser.NxParser;

public class ExecuteTransactions 
{
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	// data structure to keep track of what kind of locks are held by different entities (transactional context)
	protected static ConcurrentHashMap<String, LockEnt> lock_map = new ConcurrentHashMap<String, LockEnt>();

        // keep here only locks for the entire entity
        protected static ConcurrentHashMap<String, LockEnt> full_entity_lock_map = new ConcurrentHashMap<String, LockEnt>();

        public static final Integer TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS = 10;

        // in case the granularity is changed, a cleanup of locks may be required
        public static void resetLocks() {
            ExecuteTransactions.lock_map.clear();
            ExecuteTransactions.full_entity_lock_map.clear();
        }

	// returns the key used in the lock table
	// just one record with this key can exist in the table at same time
	private String getKey(Operation op) {
                switch( op.getType() ) {
                        case INSERT:
                        case INSERT_LINK:
                        case UPDATE:
                        case UPDATE_LINK:
                        case DELETE:
                        case DELETE_LINK:
                            if( Listener.DEFAULT_TRANS_LOCKING_GRANULARITY.equals("1") )
                                return op.getParam(3)+op.getParam(0);  // (g, e)
                            else if( Listener.DEFAULT_TRANS_LOCKING_GRANULARITY.equals("2") )
                                return op.getParam(3)+op.getParam(0)+op.getParam(1);
                            else
                                return op.getParam(3)+op.getParam(0)+op.getParam(1)+op.getParam(2);
                        case ENT_DEEP_CLONE:
                        case ENT_SHALLOW_CLONE:
                        case ENT_DELETE:
                            return op.getParam(1)+op.getParam(0);
                        default:
                            // THIS MUST not be reached!
                            return "";
                }
	}

        // the key used to lock a full entity, mostly used by copy operation
        private String getFullEntityKey(Operation op) {
              switch( op.getType() ) {
                        case INSERT:
                        case INSERT_LINK:
                        case UPDATE:
                        case UPDATE_LINK:
                        case DELETE:
                        case DELETE_LINK:
                            return op.getParam(3)+op.getParam(0);
                        case ENT_DEEP_CLONE:
                        case ENT_SHALLOW_CLONE:
                        case ENT_DELETE:
                            return op.getParam(1)+op.getParam(0);
                        default:
                            // THIS MUST not be reached!
                            return "";
                }
        }

        public int executeTransaction_MVCC(Transaction t, AbstractCassandraRdfHector store) throws Exception {
            // create a batch of all operations
            // !!! TRANSACTION MUST CONTAIN SAME TYPE OF OPERATIONS OVER SAME GRAPH
            List<Node[]> batch = new ArrayList<Node[]>();
            String keyspace = null;
            for( int j=0; j < t.ops.size(); ++j) {
                Operation c_op = t.ops.get(j);
                if( c_op.just_for_locking )
                    continue;
                
                String triple="";
                for( int i=0; i<c_op.params.length; ++i) {
                    if( i != 3 )
                        triple += c_op.params[i] + " ";
                    else
                        if( keyspace == null )
                            keyspace = Store.encodeKeyspace(c_op.params[3]);
                }
                triple += "."; 
                Node[] nx = NxParser.parseNodes(triple);
                batch.add(nx);
            }

            int r=0;
            String URN_author = t.getURN();
            // get transaction ID from Snowflake here
            String txID = Listener.SNOWFLAKE_GENERATOR.getStringId();

            // put this txID into "NOT_YET_COMMITED" list
            // this is neccessary in case a multiple-e tx is half way commited, so
            //it won't be half-way read
            store.addCIDToPendingTXList(keyspace, txID);
            
            switch( t.txType ) {
                // insert property
                case IP:
                    r = store.addDataVersioning(batch.iterator(), keyspace,
                            0, URN_author, txID);
                    break;
                // update property
                case UP:
                    r = store.updateDataVersioning(batch.iterator(), keyspace,
                            0, URN_author, txID);
                    break;
                // delete property
                case DP:
                    break;
                // delete entity
                case DE:
                    break;
                // insert link
                case IL:
                     r = store.addDataVersioning(batch.iterator(), keyspace,
                             1, URN_author, txID);
                    break;
                // delete link
                case DL:
                    break;
                // shallow copy
                case SC:
                    break;
                // deep copy
                case DC:
                    break;
                default:
                    break;
            }

             // as the tx was finished, then remove it from pending list
            store.removeCIDFromPendingTXList(keyspace, txID);
            
            switch(r) {
                case -3:
                    // exception has been thrown
                    break;
                case -2:
                    _log.info("COMMIT "+t.txType.toString()+" FAILED " +
                            "- keyspace " + keyspace + "does not exist!");
                    break;
                case -1:
                    _log.info("COMMIT "+t.txType.toString()+" FAILED - " +
                            "it has been aborted!" );
                    break;
                case 1:
                    /*_log.info("COMMIT "+t.txType.toString()+" SUCCESSFUL " +
                            "- it has been committed!" );*/
                    break;
                case 2:
                    _log.info("COMMIT "+t.txType.toString()+" FAILED - cannot " +
                            "fetch all previous versions with CID<txID" +
                            "; it means that another tx has added in the meantime " +
                            "another version with higher CID!" );
                    break;
                default:
                    break;
            }

                                    /*
                        case INSERT_LINK:
                                r = store.addData(c_op.params[0], c_op.params[1],
                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]), 1);
                                if ( r != 0 )
                                        _log.info("COMMIT INSERT LINK " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case UPDATE:
                                r = store.updateData(c_op.params[0], c_op.params[1],
                                   c_op.params[4], c_op.params[2],
                                   Store.encodeKeyspace(c_op.params[3]), 0);
                                if ( r != 0 )
                                        _log.info("COMMIT UPDATE " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case UPDATE_LINK:
                                r = store.updateData(c_op.params[0], c_op.params[1],
                                   c_op.params[4], c_op.params[2],
                                   Store.encodeKeyspace(c_op.params[3]), 1);
                                if ( r != 0 )
                                        _log.info("COMMIT UPDATE LINK " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case DELETE:
                                r = store.deleteData(c_op.params[0], c_op.params[1],
                                        c_op.params[2],
                                        Store.encodeKeyspace(c_op.params[3]), 0);
                                if ( r != 0 )
                                        _log.info("COMMIT DELETE " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case DELETE_LINK:
                                r = store.deleteData(c_op.params[0], c_op.params[1],
                                        c_op.params[2],
                                        Store.encodeKeyspace(c_op.params[3]), 1);
                                if ( r != 0 )
                                        _log.info("COMMIT DELETE LINK " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case ENT_SHALLOW_CLONE:
                                r = store.shallowClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]), c_op.params[1],
                                        c_op.params[3] );
                                if ( r != 0 )
                                    _log.info("COMMIT SHALLOW CLONE " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case ENT_DEEP_CLONE:
                                r = store.deepClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]));
                                if ( r != 0 )
                                    _log.info("COMMIT DEEP CLONE " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                        case ENT_DELETE:
                                r = store.deleteByRowKey(c_op.params[0], Store.encodeKeyspace(c_op.params[1]), 1);
                                if ( r != 0 )
                                    _log.info("COMMIT ENTITY DELETE " + c_op.params[0] + " FAILED with exit code " + r);
                                break;
                                     
                        default:
                                break;
                }*/
            return r;
        }



        // USE CAGE (ZOOKEEPER) FOR LOCKING
        public int executeTransaction_Zookeeper(Transaction t, AbstractCassandraRdfHector store) throws Exception
	{
		if( t == null )
			return -1;
		//t.printTransaction();

		// local data-structure per transaction
		// keep here track of all acquired locks during this transaction
		Hashtable<String, LockEnt> locks_hold = new Hashtable<String, LockEnt>();

                // keep track here of all the locks over the entire entity (important for copy operations and delete all)
                // NOTE: a ReadLock here means that either a read or write lock exists on one of this entity properties
                // NOTE: a WriteLock here means exclusive access at the level of the entity, no other locks can co-exist
                Hashtable<String, LockEnt> full_locks_hold = new Hashtable<String, LockEnt>();

                // CURATOR locks; we need to store this as creating another InterProcessReadWriteLock object when we want to
                //release it, does not work!!!
                Hashtable<String, InterProcessReadWriteLock> curator_locks_hold = new Hashtable<String, InterProcessReadWriteLock>();
                InterProcessReadWriteLock tmp_lock;

		try {
			// first of all lock all entities involved in this transaction; then execute
                        // NOTE: for assuring the lock for managing links, some 'dummy' operations were added
                        //just for this purpose; however, they are not run as the actions themselves are
                        //integrated into addData, insertData and so on
			for(Iterator it = t.getOps().iterator(); it.hasNext(); ) {
				Operation op = (Operation) it.next();
				String key = this.getKey(op);
                                String key_e = this.getFullEntityKey(op);

                                /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                // maybe a previous operation has already acquired the needed lock, check this
				LockEnt prev_full_lock = full_locks_hold.get(key_e);
				if( prev_full_lock != null && prev_full_lock.type == LockEnt.LockType.WRITE_LOCK )
                                    // don't need anything else as a previous operation has acquired
                                    // an exclusive lock over the whole entity
                                    continue;
                                /**END**/

				// maybe a previous operation has already acquired the needed lock, check this
				LockEnt prev_lock = locks_hold.get(key);
				if( prev_lock != null && prev_lock.type == LockEnt.LockType.WRITE_LOCK )
					// don't need anything else as a previous operation has acquired the exclusive lock
					continue;

				// NOW TRY TO ACQUIRE THE NEEDED LOCK
                                switch( op.getType() ) {
                                    case GET:
                                        // for GET we need a read lock
					if( prev_lock != null && prev_lock.type == LockEnt.LockType.READ_LOCK )
						// lock has been acquired already for this entity, go to next operation
						continue;
                                                   
                                        // try to get read lock
                                        tmp_lock = new InterProcessReadWriteLock(Listener.curator_client, "/epv/"+key);
                                        if( ! tmp_lock.readLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS, TimeUnit.MILLISECONDS) ) {
                                            // this may fail in case another Transaction changed this lock
                                            _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                       " (cannot get a READ_LOCK as another changed it in the" +
                                                       " meanwhile; restart the whole transaction!)");
                                            return 2;
                                        }
                                        else {
                                            // keep local track about locks acquired
                                            locks_hold.put(key, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
                                            curator_locks_hold.put("/epv/"+key, tmp_lock);

                                            /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                            // now try to get a read lock on the entire entity, if still exist
                                            // write lock is only used by copy operation to signal exclusive lock
                                            tmp_lock = new InterProcessReadWriteLock(Listener.curator_client, "/e/"+key_e);
                                            if( ! tmp_lock.readLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS, TimeUnit.MILLISECONDS) ) {
                                                // so again a conflict, another T may have changed either the counter or the entire lock;
                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                        "(cannot acquire a READ_LOCK on the FULL entity lock map as aonther " +
                                                        "changed it in the meanwhile; restart the whole transaction!)");
                                                return 22;
                                            }
                                            else {
                                                // keep local track of the lock acquired
                                                full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
                                                curator_locks_hold.put(key_e, tmp_lock);
                                                continue;
                                            }
                                        }   
                                case INSERT:
                                case INSERT_LINK:
                                case UPDATE:
                                case UPDATE_LINK:
                                case DELETE:
                                case DELETE_LINK:
                                case LOCK:
					// operation != GET, so here we want an EXCLUSIVE WRITE LOCK

                                        // create a READ_LOCK or increment its counter on the full entity locking map
                                        tmp_lock = new InterProcessReadWriteLock(Listener.curator_client, "/e/"+key_e);
                                        if( ! tmp_lock.readLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS, TimeUnit.MILLISECONDS) ) {
                                            // so again a conflict, another T may have already added a lock
                                            _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                " (cannot add new READ_LOCK on FULL lock entity map as there may already " +
                                                "be added by another concurrent transaction; restart the whole transaction!)");
                                            return 27;
                                        }
                                        else {
                                            full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
                                            curator_locks_hold.put("/e/"+key_e, tmp_lock);

                                            tmp_lock = new InterProcessReadWriteLock(Listener.curator_client, "/epv/"+key);
                                            if( ! tmp_lock.writeLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS, TimeUnit.MILLISECONDS) ) {
                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                          "(cannot create write lock as another one did it already; restart it)");
                                                return 7;
                                            }
                                            else {
                                                // keep local track about locks acquired
                                                locks_hold.put(key, new LockEnt(LockEnt.LockType.WRITE_LOCK));
                                                curator_locks_hold.put("/epv/"+key, tmp_lock);
                                            }
                                        }
                                        /** END **/
					break;
                                case ENT_SHALLOW_CLONE:
                                case ENT_DEEP_CLONE:
                                case ENT_DELETE:
                                        tmp_lock = new InterProcessReadWriteLock(Listener.curator_client, "/e/"+key_e);
                                        if( ! tmp_lock.writeLock().acquire(TIMEOUT_ACQUIRE_LOCKS_ZOOKEEPER_MS, TimeUnit.MILLISECONDS) ) {
                                            _log.info("CONFLICT: transaction " + t.ID + " @ operation with key "
                                                     + key_e + " (cannot acquire to WRITE_LOCK to the FULL entity locking map" +
                                                     " as another reader is present; restart the whole transaction)");
                                            return 31;
                                        }
                                        else {
                                            // keep local track of the locks
                                            full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.WRITE_LOCK));
                                            curator_locks_hold.put("/e/"+key_e, tmp_lock);
                                        }
                                        break;
                                    default:
                                       _log.info("UNKNOWN operation part of transaction " + t.ID);
                                       break;
                                }
			}

			/*// here, all locks are held
                        this.print(curator_locks_hold, t);
			try {
				_log.info("SLEEEEEEEEEEEEEEEEEEEPPPPPPPP......10s.....");
				Thread.sleep(10000);
			} catch (Exception e ) { }
			_log.info("WAKE UP !!");*/


			// run the ops
			int r=0;
			for( int j=0; j < t.ops.size(); ++j) {
				Operation c_op = t.ops.get(j);
                                if( c_op.just_for_locking )
                                    continue;
				switch(c_op.getType()) {
					case GET:
						// perform just a read (well, perpahs not so common used as there are
						// no if-else structures into the transaction flow)
						_log.info("TRANSACTION CONTEXT: GET operations are not supported");
						break;
					case INSERT:
						r = store.addData(c_op.params[0], c_op.params[1],
							c_op.params[2], Store.encodeKeyspace(c_op.params[3]), 0);
						if ( r != 0 )
							_log.info("COMMIT INSERT " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case INSERT_LINK:
						r = store.addData(c_op.params[0], c_op.params[1],
							c_op.params[2], Store.encodeKeyspace(c_op.params[3]), 1);
						if ( r != 0 )
							_log.info("COMMIT INSERT LINK " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					case UPDATE:
						r = store.updateData(c_op.params[0], c_op.params[1],
						   c_op.params[4], c_op.params[2],
						   Store.encodeKeyspace(c_op.params[3]), 0);
						if ( r != 0 )
							_log.info("COMMIT UPDATE " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case UPDATE_LINK:
						r = store.updateData(c_op.params[0], c_op.params[1],
						   c_op.params[4], c_op.params[2],
						   Store.encodeKeyspace(c_op.params[3]), 1);
						if ( r != 0 )
							_log.info("COMMIT UPDATE LINK " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					case DELETE:
						r = store.deleteData(c_op.params[0], c_op.params[1],
							c_op.params[2],
							Store.encodeKeyspace(c_op.params[3]), 0);
						if ( r != 0 )
							_log.info("COMMIT DELETE " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case DELETE_LINK:
						r = store.deleteData(c_op.params[0], c_op.params[1],
							c_op.params[2],
							Store.encodeKeyspace(c_op.params[3]), 1);
						if ( r != 0 )
							_log.info("COMMIT DELETE LINK " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case ENT_SHALLOW_CLONE:
                                                r = store.shallowClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]), c_op.params[1],
                                                        c_op.params[3] );
                                                if ( r != 0 )
                                                    _log.info("COMMIT SHALLOW CLONE " + c_op.params[0] + " FAILED with exit code " + r);
                                                break;
                                        case ENT_DEEP_CLONE:
                                                r = store.deepClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]));
                                                if ( r != 0 )
                                                    _log.info("COMMIT DEEP CLONE " + c_op.params[0] + " FAILED with exit code " + r);
                                                break;
                                        case ENT_DELETE:
                                                r = store.deleteByRowKey(c_op.params[0], Store.encodeKeyspace(c_op.params[1]), 1);
                                                if ( r != 0 )
                                                    _log.info("COMMIT ENTITY DELETE " + c_op.params[0] + " FAILED with exit code " + r);
                                                break;
					default:
						break;
				}
				if( r != 0 ) {
					// ROLLBACK !!!
					// operation failed, rollback all the previous ones
					for( int k=j-1; k>=0; --k ) {
						Operation p_op = t.getReverseOp(k);
                                                if( p_op.just_for_locking )
                                                    continue;
						switch(p_op.getType()) {
							case GET:
								// no reverse op for t
								break;
							case INSERT:
								r = store.addData(p_op.params[0], p_op.params[1],
									p_op.params[2], Store.encodeKeyspace(p_op.params[3]), 0);
								if ( r != 0 )
									_log.info("ROLLBACK INSERT " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case INSERT_LINK:
								r = store.addData(p_op.params[0], p_op.params[1],
									p_op.params[2], Store.encodeKeyspace(p_op.params[3]), 1);
								if ( r != 0 )
									_log.info("ROLLBACK INSERT LINK " + p_op.params[0] + " FAILED with exit code " + r);
								break;
							case UPDATE:
								r = store.updateData(p_op.params[0], p_op.params[1],
								   p_op.params[4], p_op.params[2],
								   Store.encodeKeyspace(p_op.params[3]), 0);
								if ( r != 0 )
									_log.info("ROLLBACK UPDATE " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case UPDATE_LINK:
								r = store.updateData(p_op.params[0], p_op.params[1],
								   p_op.params[4], p_op.params[2],
								   Store.encodeKeyspace(p_op.params[3]), 1);
								if ( r != 0 )
									_log.info("ROLLBACK UPDATE LINK " + p_op.params[0] + " FAILED with exit code " + r);
								break;
							case DELETE:
								r = store.deleteData(p_op.params[0], p_op.params[1],
									p_op.params[2],
									Store.encodeKeyspace(p_op.params[3]), 0);
								if ( r != 0 )
									_log.info("ROLLBACK DELETE " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case DELETE_LINK:
								r = store.deleteData(p_op.params[0], p_op.params[1],
									p_op.params[2],
									Store.encodeKeyspace(p_op.params[3]), 1);
								if ( r != 0 )
									_log.info("ROLLBACK DELETE LINK " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case ENT_SHALLOW_CLONE:
                                                                r = store.deleteShallowClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]), c_op.params[1],
                                                                        c_op.params[3] );
								if ( r != 0 )
									_log.info("ROLLBACK SHALLOW CLONE " + p_op.params[0] + " FAILED with exit code " + r);
                                                                break;
                                                        case ENT_DEEP_CLONE:
                                                                r = store.deleteDeepClone(c_op.params[2], Store.encodeKeyspace(c_op.params[3]));
                                                                if ( r != 0 )
                                                                        _log.info("ROLLBACK DEEP CLONE " + p_op.params[0] + " FAILED with exit code " + r);
                                                                break;
                                                        case ENT_DELETE:
                                                                _log.info("ROLLBACK for a full entity deletion is not yet supported!");
                                                                break;
							default:
								break;

						}
					}
					if( r != 0 )
						// problems while rollback-ing
						return 100;
					//rollback has occured successfully
					return 10;
				}
			}
			// COMMIT !!!
			return 0;
		}
		finally {
			// release the locks the reverse way they were acquired
			for( int i = t.getOps().size()-1; i>=0; --i) {
				Operation op_r = t.getOps().get(i);
				String key = this.getKey(op_r);
                                String key_e = this.getFullEntityKey(op_r);

				LockEnt used_lock = locks_hold.get(key);
				if( used_lock != null ) {
                                    if( used_lock.type == LockEnt.LockType.WRITE_LOCK )
                                        curator_locks_hold.get("/epv/"+key).writeLock().release();
                                    else
                                        curator_locks_hold.get("/epv/"+key).readLock().release();
                                    locks_hold.remove(key);
                                    curator_locks_hold.remove("/epv/"+key);
				}

                                /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                // remove the exclusive WRITE LOCK over a full entity
                                // or decremet a read lock counter
                                used_lock = full_locks_hold.get(key_e);
                                if( used_lock != null ) {
                                    if( used_lock.type == LockEnt.LockType.WRITE_LOCK ) 
                                        curator_locks_hold.get("/e/"+key_e).writeLock().release();
                                    else
                                        curator_locks_hold.get("/e/"+key_e).readLock().release();
                                    full_locks_hold.remove(key_e);
                                    curator_locks_hold.remove("/e/"+key_e);
                                }
                                /** END **/
			}
                        /*_log.info("AFTER RELEASING: ");
                        this.print(locks_hold, t);*/
		}
	}



       /**
        * 
	* return codes: 
	*    1  - another transaction already acquired a write lock for one of the triples
	*
 	*    2  - GET operation and incremend counter for READ_LOCK could not be set;
	*	  another T may have change the 'prev_lock';
	*
	*    3  - GET operation add NEW READ_LOCK could not be done; another T may have 
	*	  added another lock in the meanwhile; 
	*
	*    4  - !GET operation upgrade to WRITE_LOCK failed; another T may have changed 
	*	   the expected old lock;
	*
	*    5  - !GET operation cannot get exclusive WRITE_LOCK as a READ_LOCK is present
	*
	*    6  - !GET operation cannot get exclusive WRITE_LOCK as another WRITE_LOCK is present
	*	
	*    7  - !GET operation cannot create new WRITE_LOCK as maybe another T has already 
	*	  done it in the meanwhile
	*
        *    22 -
        *
        *    23 -
        *
        *    25 -
        *
        *    27 -
        *
        *    28 -
        *
        *    29 -
        *
        *    30 -
        *
        *    31 -
        *
	*    32 -
        * 
	*     0 - COMMIT succeeded
	*    10 - ROLLBACK succeeded 
	*   100 - ROLLBACK failed
	*/
	public int executeTransaction(Transaction t, AbstractCassandraRdfHector store) 
	{
		if( t == null ) 
			return -1;
		//t.printTransaction();
		
		// local data-structure per transaction
		// keep here track of all acquired locks during this transaction
		Hashtable<String, LockEnt> locks_hold = new Hashtable<String, LockEnt>();

                // keep track here of all the locks over the entire entity (important for copy operations and delete all)
                // NOTE: a ReadLock here means that either a read or write lock exists on one of this entity properties
                // NOTE: a WriteLock here means exclusive access at the level of the entity, no other locks can co-exist
                Hashtable<String, LockEnt> full_locks_hold = new Hashtable<String, LockEnt>();

		try { 	
			// first of all lock all entities involved in this transaction; then execute
                        //note: for assuring the lock for managing links, some 'dummy' operations were added
                        //just for this purpose; however, they are not run as the actions themselves are
                        //integrated into addData, insertData and so on
			for(Iterator it = t.getOps().iterator(); it.hasNext(); ) { 
				Operation op = (Operation) it.next(); 
				String key = this.getKey(op);
                                String key_e = this.getFullEntityKey(op);

                                /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                // maybe a previous operation has already acquired the needed lock, check this
				LockEnt prev_full_lock = full_locks_hold.get(key_e);
				if( prev_full_lock != null && prev_full_lock.type == LockEnt.LockType.WRITE_LOCK )
					// don't need anything else as a previous operation has acquired
                                        // an exclusive lock over the whole entity 
					continue;
                                /**END**/
				// maybe a previous operation has already acquired the needed lock, check this 
				LockEnt prev_lock = locks_hold.get(key);
				if( prev_lock != null && prev_lock.type == LockEnt.LockType.WRITE_LOCK ) 
					// don't need anything else as a previous operation has acquired the exclusive lock
					continue;


				//_log.info("Try to acquire lock for key: " + key);
				// check if another T acquired write lock on the same key
				LockEnt prev_val = ExecuteTransactions.lock_map.get(key);
				if( prev_val != null && prev_val.type == LockEnt.LockType.WRITE_LOCK ) {
					// conflict 
					_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key);
					return 1;
				}

				// NO WRITE LOCK ACQUIRED BY ANOTHER T, TRY TO ACQUIRE THE NEEDED LOCK
                                switch( op.getType() ) {
                                    case GET:
                                        // for GET we need a read lock
					if( prev_lock != null && prev_lock.type == LockEnt.LockType.READ_LOCK )
						// lock has been acquired already for this entity, go to next operation
						continue;

					// read again the already acquired lock with this key
					prev_val = ExecuteTransactions.lock_map.get(key);	
					if( prev_val != null ) {
						// increment the previous read lock counter 
						LockEnt new_lock = new LockEnt(LockEnt.LockType.READ_LOCK, prev_val.counter.get()+1);
						if( ExecuteTransactions.lock_map.replace(key, prev_val, new_lock) == false ) {
							// this may fail in case another Transaction changed this lock 
							_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
								   " (cannot increment a READ_LOCK counter as another changed it in the" +
								   " meanwhile; restart the whole transaction!)");
							return 2; 
						}
						else {
                                                    // keep local track about locks acquired
                                                    locks_hold.put(key, new_lock);

                                                    /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                                    // now try to increment the read lock on the entire entity, if still exist
                                                    // write lock is only used by copy operation to signal exclusive lock
                                                    prev_full_lock = ExecuteTransactions.full_entity_lock_map.get(key_e);
                                                    if( prev_full_lock != null && prev_full_lock.type == LockEnt.LockType.READ_LOCK ) {
                                                        // increment the counter
                                                        new_lock = new LockEnt(LockEnt.LockType.READ_LOCK, prev_full_lock.counter.get()+1);
                                                        if( ExecuteTransactions.full_entity_lock_map.replace(key_e, prev_full_lock, new_lock) == false ) {
                                                                // so again a conflict, another T may have changed either the counter or the entire lock;
                                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                                        "(cannot increment a READ_LOCK on the FULL entity lock map as aonther " +
                                                                        "changed it in the meanwhile; restart the whole transaction!)");
                                                                return 22;
                                                        }
                                                        else {
                                                                // keep local track of the lock acquired
                                                                full_locks_hold.put(key_e, new_lock);
                                                                continue;
                                                        }
                                                    }
                                                    /**END**/
                                                }
					}
					else {// previous value is empty!
						// just add the read lock object (no one existed before)
						if( ExecuteTransactions.lock_map.putIfAbsent(key, new LockEnt(LockEnt.LockType.READ_LOCK, 1)) != null ) {
							_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
								  " (cannot add new READ_LOCK as there may already be added by another" +
								  " concurrent transaction; restart the whole transaction!)");
							return 3;
						}
						else {
                                                    // keep local track about locks acquired
                                                    locks_hold.put(key, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
                                                    
                                                    /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                                    // now try to add the read lock on the entire entity, if still does not exist
                                                    if( ExecuteTransactions.full_entity_lock_map.putIfAbsent(key_e, 
                                                            new LockEnt(LockEnt.LockType.READ_LOCK, 1) ) != null ) {
                                                            // so again a conflict, another T may have already added a lock
                                                            _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
								  " (cannot add new READ_LOCK on FULL lock entity map as there may already " +
                                                                  "be added by another concurrent transaction; restart the whole transaction!)");
                                                            return 23;
                                                        }
                                                    else {
                                                            // keep local track about the lock acquired
                                                            full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
                                                            continue;
                                                    }
                                                    /**END**/
                                                }
					}
                                        break;
                                case INSERT:
                                case INSERT_LINK:
                                case UPDATE:
                                case UPDATE_LINK:
                                case DELETE:
                                case DELETE_LINK:
                                case LOCK:
					// operation != GET, so here we want an EXCLUSIVE WRITE LOCK
					if( prev_lock != null ) { 
						if ( prev_lock.type == LockEnt.LockType.WRITE_LOCK )
							// lock has been acquired already for this entity, go to next transaction 
							continue;
						else if ( prev_lock.type == LockEnt.LockType.READ_LOCK ) {
							// upgrade to write lock if we are the only reader 
							if( ExecuteTransactions.lock_map.replace(key, 
								new LockEnt(LockEnt.LockType.READ_LOCK, prev_lock.counter.get()), 
								new LockEnt(LockEnt.LockType.WRITE_LOCK)) == false ) { 
								// it failed (there is another reader besides me) 
								_log.info("CONFLICT: transaction " + t.ID + " @ operation with key "
									 + key + " (cannot upgrade to WRITE_LOCK as another reader " +
									" is present; restart the whole transaction)");
								return 4;
							}
							else { 
								// keep local track of this updated lock (the old value is replaced)
								locks_hold.put(key, new LockEnt(LockEnt.LockType.WRITE_LOCK));
                                                                 /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                                                // do nothing in this case, a read lock on the full entity map must exist already
                                                                /**END**/
								// don't bother anymore as we have now the right lock 
								continue; 
							}
						}
					}
					// read again the already acquired lock with this key (as it may have changed since previous same call)
                                        /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                        prev_full_lock = ExecuteTransactions.full_entity_lock_map.get(key_e);
                                        if( prev_full_lock != null && prev_full_lock.type == LockEnt.LockType.WRITE_LOCK ) {
                                                //conflict, write lock is exclusive, it means a copy operation is running now
                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                          " (cannot get an exclusive WRITE_LOCK as a WRITE_LOCK already exists" +
                                                          "on the entire entity locking map;"+
                                                          " restart the whole transaction;)");
                                                return 25;
                                        }
                                        /**END**/

					prev_val = ExecuteTransactions.lock_map.get(key);	
					if( prev_val != null && prev_val.type == LockEnt.LockType.READ_LOCK ) {
						//conflict, write lock is exclusive 
						_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
							  " (cannot get an exclusive WRITE_LOCK as a READ_LOCK already exists;"+
							  " restart the whole transaction;)");
						return 5; 
					}

					if( prev_val != null && prev_val.type == LockEnt.LockType.WRITE_LOCK ) {
                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
							  " (write lock alreay exists but it is taken by another transaction;"+
							  " restart the whole transaction;)");
						return 6;
					}
					else {
						// prev_val == null
						if ( ExecuteTransactions.lock_map.putIfAbsent(key, 
                                                        new LockEnt(LockEnt.LockType.WRITE_LOCK)) != null ) {
                                                        _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                                  "(cannot create write lock as another one did it already; restart it)");
                                                        return 7;
						}
						else {
                                                        // keep local track about locks acquired
                                                        locks_hold.put(key, new LockEnt(LockEnt.LockType.WRITE_LOCK));
                                                        
                                                        /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                                        // create a READ_LOCK or increment its counter on the full entity locking map 
                                                        prev_val = ExecuteTransactions.full_entity_lock_map.get(key_e);
                                                        if( prev_val == null ) {
                                                                if( ExecuteTransactions.full_entity_lock_map.putIfAbsent(key_e, 
                                                                        new LockEnt(LockEnt.LockType.READ_LOCK, 1) ) != null ) {
                                                                        // so again a conflict, another T may have already added a lock
                                                                        _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
                                                                            " (cannot add new READ_LOCK on FULL lock entity map as there may already " +
                                                                            "be added by another concurrent transaction; restart the whole transaction!)");
                                                                        return 27;
                                                                }
                                                                else {
                                                                        full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
                                                                }
                                                        }
                                                        else if ( prev_val.type == LockEnt.LockType.READ_LOCK ) {
                                                                // then just increment the counter
                                                                LockEnt new_lock = new LockEnt(LockEnt.LockType.READ_LOCK, prev_val.counter.get()+1);
                                                                if( ExecuteTransactions.full_entity_lock_map.replace(key_e, prev_val, new_lock) == false ) {
                                                                        // so again a conflict, another T may have changed either the counter or the
                                                                        // entire lock; a restart is needed here
                                                                        _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                                            "(cannot increment a READ_LOCK on the FULL entity lock map as aonther " +
                                                                            "changed it in the meanwhile; restart the whole transaction!)");
                                                                        return 28;
                                                                }
                                                                else {
                                                                    // keep local track about the lock acquired
                                                                    full_locks_hold.put(key_e, new_lock);
                                                                }
                                                        }
                                                        else {
                                                                // so again a conflict, another T may have acquired a write lock exclusive
                                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                                    "(cannot create a READ_LOCK on the FULL entity lock map as aonther " +
                                                                    "has acquired a WRITE_LOCK it in the meanwhile; restart the whole transaction!)");
                                                                return 29;
                                                        }
                                                        /** END **/
                                                }
					}
					break;
                                case ENT_SHALLOW_CLONE:
                                case ENT_DEEP_CLONE:
                                case ENT_DELETE:
                                        /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                        // COPY_ALL operations, so get a WRITE_LOCK on the entire entity if none READ LOCKS
                                        // OR ENTITY_DELETE operation, same locks must be acquired
                                        prev_full_lock = ExecuteTransactions.full_entity_lock_map.get(key_e);
                                        if( prev_full_lock != null) {
                                            if( prev_full_lock.type == LockEnt.LockType.WRITE_LOCK ) {
                                                // another one has already acquired it, so quit
                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                    "(cannot create a WRITE_LOCK on the FULL entity lock map as aonther " +
                                                    "has acquired a WRITE_LOCK it in the meanwhile; restart the whole transaction!)");
                                                return 30;
                                            }
                                            else { // so there is a read lock
                                                prev_lock = full_locks_hold.get(key_e);
                                               if( prev_lock != null && prev_lock.type == LockEnt.LockType.READ_LOCK ) {
                                                    // upgrade to write lock if we are the only reader
                                                    if( ExecuteTransactions.full_entity_lock_map.replace(key_e,
                                                            new LockEnt(LockEnt.LockType.READ_LOCK, 1),
                                                            new LockEnt(LockEnt.LockType.WRITE_LOCK)) == false ) {
                                                            // it failed (there is another reader besides me)
                                                            _log.info("CONFLICT: transaction " + t.ID + " @ operation with key "
                                                                     + key + " (cannot upgrade to WRITE_LOCK to the FULL entity locking map" +
                                                                     " as another reader is present; restart the whole transaction)");
                                                            return 31;
                                                    }
                                                    else {
                                                        // keep local track of the locks
                                                        full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.WRITE_LOCK));
                                                    }
                                                }
                                                // WRITE_LOCK can also be present, but this check has been done at the beginning, so skip it here
                                            }
                                        }
                                        else {
                                            // so no one has a READ or WRITE lock
                                            // no it's time to create a WRITE exclusive lock
                                            if( ExecuteTransactions.full_entity_lock_map.putIfAbsent(key_e,
                                                new LockEnt(LockEnt.LockType.WRITE_LOCK) ) != null ) {
                                                // so again a conflict, another T may have already added a lock
                                                _log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key +
                                                    " (cannot add new WRITE_LOCK on FULL lock entity map as there may already " +
                                                    "be added by another concurrent transaction; restart the whole transaction!)");
                                                return 32;
                                            }
                                            else
                                                    // keep local track of the locks
                                                    full_locks_hold.put(key_e, new LockEnt(LockEnt.LockType.WRITE_LOCK));
                                        /** END **/
                                        }
                                        break;
                                    default:
                                           _log.info("UNKNOWN operation part of transaction " + t.ID);
                                           break;
                                }
			}
			/*// here, all locks are held
                        this.print(locks_hold, t);
			try { 
				_log.info("SLEEEEEEEEEEEEEEEEEEEPPPPPPPP...........");
				Thread.sleep(2000);
			} catch (Exception e ) { }
			_log.info("WAKE UP !!");*/


			// run the ops 
			int r=0;
			for( int j=0; j < t.ops.size(); ++j) { 
				Operation c_op = t.ops.get(j);
                                if( c_op.just_for_locking )
                                    continue;
				switch(c_op.getType()) { 	
					case GET: 
						// perform just a read (well, perpahs not so common used as there are 
						// no if-else structures into the transaction flow)
						_log.info("TRANSACTION CONTEXT: GET operations are not supported");
						break;
					case INSERT:
						r = store.addData(c_op.params[0], c_op.params[1],
							c_op.params[2], Store.encodeKeyspace(c_op.params[3]), 0);
						if ( r != 0 ) 
							_log.info("COMMIT INSERT " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case INSERT_LINK:
						r = store.addData(c_op.params[0], c_op.params[1],
							c_op.params[2], Store.encodeKeyspace(c_op.params[3]), 1);
						if ( r != 0 )
							_log.info("COMMIT INSERT LINK " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					case UPDATE:
						r = store.updateData(c_op.params[0], c_op.params[1],
						   c_op.params[4], c_op.params[2], 
						   Store.encodeKeyspace(c_op.params[3]), 0);
						if ( r != 0 ) 
							_log.info("COMMIT UPDATE " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case UPDATE_LINK:
						r = store.updateData(c_op.params[0], c_op.params[1],
						   c_op.params[4], c_op.params[2],
						   Store.encodeKeyspace(c_op.params[3]), 1);
						if ( r != 0 )
							_log.info("COMMIT UPDATE LINK " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					case DELETE:
						r = store.deleteData(c_op.params[0], c_op.params[1],
							c_op.params[2], 
							Store.encodeKeyspace(c_op.params[3]), 0);
						if ( r != 0 ) 
							_log.info("COMMIT DELETE " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case DELETE_LINK:
						r = store.deleteData(c_op.params[0], c_op.params[1],
							c_op.params[2],
							Store.encodeKeyspace(c_op.params[3]), 1);
						if ( r != 0 )
							_log.info("COMMIT DELETE LINK " + c_op.params[0] + " FAILED with exit code " + r);
						break;
                                        case ENT_SHALLOW_CLONE:
                                                r = store.shallowClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]), c_op.params[1],
                                                        c_op.params[3] );
                                                if ( r != 0 )
                                                    _log.info("COMMIT SHALLOW CLONE " + c_op.params[0] + " FAILED with exit code " + r);
                                                break;
                                        case ENT_DEEP_CLONE:
                                                r = store.deepClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]));
                                                if ( r != 0 )
                                                    _log.info("COMMIT DEEP CLONE " + c_op.params[0] + " FAILED with exit code " + r);
                                                break;
                                        case ENT_DELETE:
                                                r = store.deleteByRowKey(c_op.params[0], Store.encodeKeyspace(c_op.params[1]), 1);
                                                if ( r != 0 )
                                                    _log.info("COMMIT ENTITY DELETE " + c_op.params[0] + " FAILED with exit code " + r);
                                                break;
					default:
						break;	
				}
				if( r != 0 ) {		
					// ROLLBACK !!! 
					// operation failed, rollback all the previous ones
					for( int k=j-1; k>=0; --k ) { 
						Operation p_op = t.getReverseOp(k);
                                                if( p_op.just_for_locking )
                                                    continue;
						switch(p_op.getType()) { 	
							case GET: 	
								// no reverse op for t
								break;
							case INSERT:
								r = store.addData(p_op.params[0], p_op.params[1],
									p_op.params[2], Store.encodeKeyspace(p_op.params[3]), 0);
								if ( r != 0 ) 
									_log.info("ROLLBACK INSERT " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case INSERT_LINK:
								r = store.addData(p_op.params[0], p_op.params[1],
									p_op.params[2], Store.encodeKeyspace(p_op.params[3]), 1);
								if ( r != 0 )
									_log.info("ROLLBACK INSERT LINK " + p_op.params[0] + " FAILED with exit code " + r);
								break;
							case UPDATE:
								r = store.updateData(p_op.params[0], p_op.params[1],
								   p_op.params[4], p_op.params[2], 
								   Store.encodeKeyspace(p_op.params[3]), 0);
								if ( r != 0 ) 
									_log.info("ROLLBACK UPDATE " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case UPDATE_LINK:
								r = store.updateData(p_op.params[0], p_op.params[1],
								   p_op.params[4], p_op.params[2],
								   Store.encodeKeyspace(p_op.params[3]), 1);
								if ( r != 0 )
									_log.info("ROLLBACK UPDATE LINK " + p_op.params[0] + " FAILED with exit code " + r);
								break;
							case DELETE:
								r = store.deleteData(p_op.params[0], p_op.params[1],
									p_op.params[2], 
									Store.encodeKeyspace(p_op.params[3]), 0);
								if ( r != 0 ) 
									_log.info("ROLLBACK DELETE " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case DELETE_LINK:
								r = store.deleteData(p_op.params[0], p_op.params[1],
									p_op.params[2],
									Store.encodeKeyspace(p_op.params[3]), 1);
								if ( r != 0 )
									_log.info("ROLLBACK DELETE LINK " + p_op.params[0] + " FAILED with exit code " + r);
								break;
                                                        case ENT_SHALLOW_CLONE:
                                                                r = store.deleteShallowClone(c_op.params[0], Store.encodeKeyspace(c_op.params[1]),
                                                                        c_op.params[2], Store.encodeKeyspace(c_op.params[3]), c_op.params[1],
                                                                        c_op.params[3] );
								if ( r != 0 )
									_log.info("ROLLBACK SHALLOW CLONE " + p_op.params[0] + " FAILED with exit code " + r);
                                                                break;
                                                        case ENT_DEEP_CLONE:
                                                                r = store.deleteDeepClone(c_op.params[2], Store.encodeKeyspace(c_op.params[3]));
                                                                if ( r != 0 )
                                                                        _log.info("ROLLBACK DEEP CLONE " + p_op.params[0] + " FAILED with exit code " + r);
                                                                break;
                                                        case ENT_DELETE:
                                                                _log.info("ROLLBACK for a full entity deletion is not yet supported!");
                                                                break;
							default:
								break;	

						}
					}	
					if( r != 0 )
						// problems while rollback-ing 
						return 100;
					//rollback has occured successfully 
					return 10;
				}
			}
			// COMMIT !!!
			return 0;
		}
		finally { 
			// release the locks the reverse way they were acquired
			for( int i = t.getOps().size()-1; i>=0; --i) { 
				Operation op_r = t.getOps().get(i);
				String key = this.getKey(op_r);
                                String key_e = this.getFullEntityKey(op_r);

				LockEnt used_lock = locks_hold.get(key);
				if( used_lock != null ) {
					if( used_lock.type == LockEnt.LockType.WRITE_LOCK ) { 
						// it was an exclusive lock, just delete it 
						ExecuteTransactions.lock_map.remove(key); 
						//_log.info("WRITE LOCK FOR KEY " + key + " HAS BEEN RELEASED ");
						locks_hold.remove(key);
					}	
					else { 
						// decrement the counter of a read lock and delete it if counter == 0 
						// run this in a loop as the 'replace' may fail due to concurrent transactions
						while( true ) { 
							// get current value 
							LockEnt curr_v = ExecuteTransactions.lock_map.get(key);
                                                        if( curr_v == null )
                                                            break;
							// try to replace 
							if( ExecuteTransactions.lock_map.replace(key, curr_v, 
								new LockEnt(LockEnt.LockType.READ_LOCK, curr_v.counter.get()-1)) == true ) {
								// remove if the counter is 0 
								ExecuteTransactions.lock_map.remove(key, new LockEnt(LockEnt.LockType.READ_LOCK, 0));
								//_log.info("READ LOCK FOR KEY " + key + " HAS BEEN RELEASED ");
								locks_hold.remove(key);
								break;
							}
						}
					}
				}

                                /** ADDED FOR SUPPORTING COPY OPERATION AND ITS ENTIRE ENTITY LOCKING **/
                                // remove the exclusive WRITE LOCK over a full entity
                                // or decremet a read lock counter
                                used_lock = full_locks_hold.get(key_e);
                                if( used_lock != null ) {
                                    if( used_lock.type == LockEnt.LockType.WRITE_LOCK ) {
						// it was an exclusive lock, just delete it
						ExecuteTransactions.full_entity_lock_map.remove(key_e);
						//_log.info("WRITE LOCK FOR KEY " + key + " HAS BEEN RELEASED ");
						full_locks_hold.remove(key_e);
					}
					else {
						// decrement the counter of a read lock and delete it if counter == 0
						// run this in a loop as the 'replace' may fail due to concurrent transactions
						while( true ) {
							// get current value
							LockEnt curr_v = ExecuteTransactions.full_entity_lock_map.get(key_e);
                                                        if( curr_v == null )
                                                            break;
							// try to replace
							if( ExecuteTransactions.full_entity_lock_map.replace(key_e, curr_v,
								new LockEnt(LockEnt.LockType.READ_LOCK, curr_v.counter.get()-1)) == true ) {
								// remove if the counter is 0
								ExecuteTransactions.full_entity_lock_map.remove(key_e, new LockEnt(LockEnt.LockType.READ_LOCK, 0));
								//_log.info("READ LOCK FOR KEY " + key + " HAS BEEN RELEASED ");
								full_entity_lock_map.remove(key_e);
								break;
							}
						}
					}
                                }
                                /** END **/
			}
                        /*_log.info("AFTER RELEASING: ");
                        this.print(locks_hold, t);*/
		}
	}

        private void print(Hashtable locks_hold, Transaction t) {
            	// print what locks this transaction holds
                StringBuffer sb = new StringBuffer();
                sb.append("Following locks are held by transaction " + t.ID+"\n");
                for( Iterator it = locks_hold.keySet().iterator(); it.hasNext(); ) {
                         String k = (String) it.next();
                         sb.append("KEY: " + k + " " + locks_hold.get(k).toString()+"\n");
                }
                sb.append("\nFollowing FULL entity locks are held by transaction " + t.ID +"\n");
                for( Iterator it = full_entity_lock_map.keySet().iterator(); it.hasNext(); ) {
                         String k = (String) it.next();
                         sb.append("KEY: " + k + " " + full_entity_lock_map.get(k).toString()+"\n");
                }
                _log.info(sb.toString());
        }
}
