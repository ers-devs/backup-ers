package edu.kit.aifb.cumulus.store;

import java.util.concurrent.atomic.AtomicInteger;
import java.lang.StringBuffer;

import java.util.logging.Logger;

public class LockEnt { 

	private final Logger _log = Logger.getLogger(this.getClass().getName());
	// many read locks can co-exist, but write lock is exclusive	
	protected enum LockType { 
		READ_LOCK, WRITE_LOCK, NO_LOCK
	}
	public LockType type; 
	public AtomicInteger counter = new AtomicInteger(0);
	public LockEnt() { 
		this.type = LockType.NO_LOCK;
	}
	public LockEnt(LockType type) { 
		this.type = type; 
		this.counter.incrementAndGet();
	}
	public LockEnt(LockType type, int init_val) { 
		this.type=type; 
		this.counter.set(init_val);
	}
	public String toString() { 
		StringBuffer buf = new StringBuffer(); 
		if( type == LockType.READ_LOCK ) { 
			buf.append("read lock");
			buf.append(" counter= "+counter.get());
		}
		else if ( type == LockType.WRITE_LOCK ) { 
			buf.append("Exclusive write lock");
		}
		else {
			buf.append("No lock");
		}
		return buf.toString();
	}

	// NOTE: this is important as otherwise the ConcurrentHashmap of AbstractCassandraRdfHector won't work propery ;) 
	public boolean equals(Object b) { 
		if( b instanceof LockEnt ) { 
			if( this.type == ((LockEnt)b).type && this.counter.get() == ((LockEnt)b).counter.get()  ) 
				return true; 
		}
		return false;
	}
}

