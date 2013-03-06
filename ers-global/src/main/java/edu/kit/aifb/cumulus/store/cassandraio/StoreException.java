package edu.kit.aifb.cumulus.store.cassandraio;


/** 
 * 
 * @author aharth
 */
public class StoreException extends Exception {
	private static final long serialVersionUID = 1L;

	public StoreException(Exception e) {
		super(e);
	}
}
