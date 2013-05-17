package edu.kit.aifb.cumulus.store; 

import edu.kit.aifb.cumulus.webapp.Listener;
import java.util.Hashtable;

public class Operation 
{
	public static enum Type {
                // clasical operations
		GET, INSERT, UPDATE, DELETE,
                // operation with links
                INSERT_LINK, UPDATE_LINK, DELETE_LINK,
                // clone and delete an entire entity
                ENT_SHALLOW_CLONE, ENT_DEEP_CLONE, ENT_DELETE,
                // generic used just for locking
                LOCK
	}

        // simple operations without link
	public static final String GET_OP_NAME = "get"; 
	public static final String INSERT_OP_NAME = "insert";
	public static final String UPDATE_OP_NAME = "update";
        public static final String DELETE_OP_NAME = "delete";
        // simple operations with link
        public static final String INSERT_LINK_OP_NAME = "insert_link";
        public static final String UPDATE_LINK_OP_NAME = "update_link";
        public static final String DELETE_LINK_OP_NAME = "delete_link";
        // complex operations that need to lock entire entity
        public static final String SHALLOW_CLONE_OP_NAME = "shallow_clone";
        public static final String DEEP_CLONE_OP_NAME = "deep_clone";
        public static final String DELETE_ALL_OP_NAME = "delete_all";
        // dummy op used just for locking by linking operations
        public static final String JUST_LOCK_OP_NAME = "lock";

	public static Hashtable<String, Type> map = new Hashtable<String,Type>();
	public static Hashtable<Type, Type> reverse = new Hashtable<Type, Type>();
	
        // each operation has a different need in terms of # of params
        public static Hashtable<Type, Integer> needed_params = new Hashtable<Type,Integer>();

        // set this to true if this operation is used only in the locking logic
        public boolean just_for_locking = false;

	static { 
		map.put(GET_OP_NAME, Type.GET);
		map.put(INSERT_OP_NAME, Type.INSERT);
                map.put(INSERT_LINK_OP_NAME, Type.INSERT_LINK);
		map.put(UPDATE_OP_NAME, Type.UPDATE);
                map.put(UPDATE_LINK_OP_NAME, Type.UPDATE_LINK);
		map.put(DELETE_OP_NAME, Type.DELETE);
                map.put(DELETE_LINK_OP_NAME, Type.DELETE_LINK);
                map.put(SHALLOW_CLONE_OP_NAME, Type.ENT_SHALLOW_CLONE);
                map.put(DEEP_CLONE_OP_NAME, Type.ENT_DEEP_CLONE);
                map.put(DELETE_ALL_OP_NAME, Type.ENT_DELETE);
                map.put(JUST_LOCK_OP_NAME, Type.LOCK);

		reverse.put(Type.GET, Type.GET); 
		reverse.put(Type.INSERT, Type.DELETE);
                reverse.put(Type.INSERT_LINK, Type.DELETE_LINK);
		reverse.put(Type.DELETE, Type.INSERT);
                reverse.put(Type.DELETE_LINK, Type.INSERT_LINK);
		reverse.put(Type.UPDATE, Type.UPDATE);
                reverse.put(Type.UPDATE_LINK, Type.UPDATE_LINK);
                reverse.put(Type.ENT_SHALLOW_CLONE, Type.ENT_DELETE);
                reverse.put(Type.ENT_DEEP_CLONE, Type.ENT_DELETE);
                reverse.put(Type.ENT_DELETE, Type.ENT_DEEP_CLONE);
                reverse.put(Type.LOCK, Type.LOCK);

                needed_params.put(Type.GET, 4);                  // (e,p,v,graph)
                needed_params.put(Type.INSERT, 4);               // (e,p,v,graph)
                needed_params.put(Type.INSERT_LINK, 4);          // (e1,p,e2,graph)
                needed_params.put(Type.UPDATE, 5);               // (e,p,v_new,graph,v_old)
                needed_params.put(Type.UPDATE_LINK, 5);          // (e,p,e_new,graph,v_old)
                needed_params.put(Type.DELETE, 4);               // (e,p,v,graph)
                needed_params.put(Type.DELETE_LINK, 4);          // (e1,p,e2,graph)
                needed_params.put(Type.ENT_SHALLOW_CLONE, 4);    // (e_src,graph_src,e_dest,graph_dest)
                needed_params.put(Type.ENT_DEEP_CLONE, 4);       // (e_src,graph_src,e_dest,graph_dest)
                needed_params.put(Type.ENT_DELETE, 2);           // (e_src,graph_src)
 	}
	
	private Type type; 
	public String[] params = new String[5];

	public Operation() { 
		init();
	}

	public Operation(Type type) { 
		this();
		this.type = type; 
	}

	private void init() { 
		for( int i=0; i<5; ++i ) { 
			params[i] = new String();
		}
	}
		
	public void addParam(String param, int pos) { 
		params[pos] = new String(param.trim());
	}
	
	public String getParam(int pos) {
		return params[pos];
	}

	public void copyParam(Operation op) { 
		for( int i=0; i<params.length; ++i ) { 
			this.addParam(op.getParam(i), i);
		}
	}

	public Type getType() { 
		return this.type;
	}

	public void swapOldNewValuesParam() {
		String tmp = params[2]; // v_new
		params[2] = params[4]; 	// v_new = v_old
		params[4] = params[2]; 	// v_old = v_old
	}

        public void prepareForLinks() {
            // swap v with e
            String tmp = params[2];
            params[2] = params[0];
            params[0] = tmp;
            tmp = params[1];
            // change P, add the Listener prefix
            if( params[1].startsWith("<") )
                params[1] = "<";
            else if( params[1].startsWith("\"") ) 
                params[1] = "\"";
            params[1] += Listener.DEFAULT_ERS_LINKS_PREFIX + tmp.substring(1);
        }

        public void swapEwithV() {
            // swap v with e
            String tmp = params[2];
            params[2] = params[0];
            params[0] = tmp;
        }

	public String printToString() { 
		StringBuffer buf = new StringBuffer("Type " + this.type);
                if( just_for_locking )
                    buf.append(" - used just for locking - ");
		buf.append(" arguments: (");
		for( int i=0; i<5; ++i ) { 
			buf.append(params[i]);
			if( i != 4 ) 
				buf.append(",");
		}
		buf.append(")\n");
		return buf.toString();
	}

}