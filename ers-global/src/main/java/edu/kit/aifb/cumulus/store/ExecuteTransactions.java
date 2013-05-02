package edu.kit.aifb.cumulus.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.logging.Logger;

public class ExecuteTransactions 
{
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	// data structure to keep track of what kind of locks are held by different entities (transactional context)
	protected static ConcurrentHashMap<String, LockEnt> lock_map = new ConcurrentHashMap<String, LockEnt>();

	// returns the key used in the lock table
	// just one record with this key can exist in the table at same time
	private String getKey(Operation op) { 
		return op.getParam(0)+op.getParam(1)+op.getParam(2); // (e,p,v)
		//return op.getParam(0); // e
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

		try { 	
			// first of all lock all entities involved in this transaction; then execute
			for(Iterator it = t.getOps().iterator(); it.hasNext(); ) { 
				Operation op = (Operation) it.next(); 
				String key = this.getKey(op);
				
				// maybe a previous operation has already acquired the needed lock, check this 
				LockEnt prev_lock = locks_hold.get(key);
				if( prev_lock != null && prev_lock.type == LockEnt.LockType.WRITE_LOCK ) 
					// don't need anything else as a previous operation has acquired the exclusive lock
					continue;

				//_log.info("Try to acquire lock for key: " + key);
				// add this to the lock map, if absent 
				LockEnt prev_val = ExecuteTransactions.lock_map.get(key);
				if( prev_val != null && prev_val.type == LockEnt.LockType.WRITE_LOCK ) {
					// conflict 
					_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
						  " (write lock already acquired by another transaction)");
					return 1;
				}

				// NO WRITE LOCK ACQUIRED BY ANOTHER T, TRY TO ACQUIRE THE NEEDED LOCK 
				if( op.getType() == Operation.Type.GET ) { 
					// operation == GET, just a READ_LOCK would suffice
					if( prev_lock != null && ( prev_lock.type == LockEnt.LockType.READ_LOCK || 
								   prev_lock.type == LockEnt.LockType.WRITE_LOCK ) ) 
						// lock has been acquired already for this entity, go to next transaction 
						continue;

					// read again the already acquired lock with this key (as it may have changed since previous 
 					// same call)
					prev_val = ExecuteTransactions.lock_map.get(key);	
					if( prev_val != null ) {
						// increment the previous read lock counter 
						LockEnt new_lock = new LockEnt(LockEnt.LockType.READ_LOCK, prev_val.counter.get()+1);
						if( ExecuteTransactions.lock_map.replace(key, prev_val, new_lock) == false ) {
							// this may fail in case another Transaction changed this lock (maybe a WRITE one
 							// or just deleted at all); a restart is needed here 
							_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
								   " (cannot increment a READ_LOCK counter as another changed it in the" +
								   " meanwhile; restart the whole transaction!)");
							return 2; 
						}
						else 	// keep local track about locks acquired
							locks_hold.put(key, new_lock);
					}
					else {
						// just add the lock object (no one existed before)
						if( ExecuteTransactions.lock_map.putIfAbsent(key, 
							new LockEnt(LockEnt.LockType.READ_LOCK, 1)) != null ) {
							_log.info("CONFLICT: transaction " + t.ID + " @ operation with key " + key + 
								  " (cannot add new READ_LOCK as there may already be added by another" +
								  " concurrent transaction; restart the whole transaction!)");
							return 3;
						}
						else 
							// keep local track about locks acquired
							locks_hold.put(key, new LockEnt(LockEnt.LockType.READ_LOCK, 1));
					}
				}
				else { 
					// operation != GET, we want an EXCLUSIVE WRITE LOCK
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
								// don't bother anymore as we have now the right lock 
								continue; 
							}
						}
					}
					// read again the already acquired lock with this key (as it may have changed since previous 
 					// same call)
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
						else 
							// keep locl track about locks acquired 
							locks_hold.put(key, new LockEnt(LockEnt.LockType.WRITE_LOCK));
					}
						
				}
			}
			// here, all locks are held 

			// print what locks this transaction holds
			/*_log.info("Following locks are held by transaction " + t.ID); 
			for( Iterator it = locks_hold.keySet().iterator(); it.hasNext(); ) { 
				 String k = (String) it.next(); 
				_log.info("KEY: " + k + " " + locks_hold.get(k).toString());
			}*/

/*			try { 
				_log.info("SLEEEEEEEEEEEEEEEEEEEPPPPPPPP...........");
				Thread.sleep(15000);	
			} catch (Exception e ) { }

			_log.info("WAKE UP !!");  */


			// run the ops 
			int r=0;
			for( int j=0; j < t.ops.size(); ++j) { 
				Operation c_op = t.ops.get(j);
				switch(c_op.getType()) { 	
					case GET: 
						// perform just a read (well, perpahs not so common used as there are 
						// no if-else structures into the transaction flow)
						_log.info("TRANSACTION CONTEXT: GET operations are not supported");
						break;
					case INSERT:
						r = store.addData(c_op.params[0], c_op.params[1],
							c_op.params[2], 
							Store.encodeKeyspace(c_op.params[3]));
						if ( r != 0 ) 
							_log.info("COMMIT INSERT " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					case UPDATE:
						r = store.updateData(c_op.params[0], c_op.params[1],
						   c_op.params[4], c_op.params[2], 
						   Store.encodeKeyspace(c_op.params[3]));
						if ( r != 0 ) 
							_log.info("COMMIT UPDATE " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					case DELETE:
						r = store.deleteData(c_op.params[0], c_op.params[1],
							c_op.params[2], 
							Store.encodeKeyspace(c_op.params[3]));
						if ( r != 0 ) 
							_log.info("COMMIT DELETE " + c_op.params[0] + " FAILED with exit code " + r);
						break;
					default:
						break;	
				}
				if( r != 0 ) {		
					// ROLLBACK !!! 
					// operation failed, rollback all the previous ones
					for( int k=j-1; k>=0; --k ) { 
						Operation p_op = t.getReverseOp(k); 
						switch(p_op.getType()) { 	
							case GET: 	
								// no reverse op for t
								break;
							case INSERT:
								r = store.addData(p_op.params[0], p_op.params[1],
									p_op.params[2], 
									Store.encodeKeyspace(p_op.params[3]));
								if ( r != 0 ) 
									_log.info("ROLLBACK INSERT " + p_op.params[0] + " FAILED with exit code " + r);
								break;
							case UPDATE:
								r = store.updateData(p_op.params[0], p_op.params[1],
								   p_op.params[4], p_op.params[2], 
								   Store.encodeKeyspace(p_op.params[3]));
								if ( r != 0 ) 
									_log.info("ROLLBACK UPDATE " + p_op.params[0] + " FAILED with exit code " + r);
								break;
							case DELETE:
								r = store.deleteData(p_op.params[0], p_op.params[1],
									p_op.params[2], 
									Store.encodeKeyspace(p_op.params[3]));
								if ( r != 0 ) 
									_log.info("ROLLBACK DELETE " + p_op.params[0] + " FAILED with exit code " + r);
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
			}
		}
	}
}
