package edu.kit.aifb.cumulus.store; 

import java.util.Hashtable;
import java.lang.String; 

public class Operation 
{
	public static enum Type {
		GET, INSERT, UPDATE, DELETE 
	}

	public static final String GET_OP_NAME = "get"; 
	public static final String INSERT_OP_NAME = "insert";
	public static final String UPDATE_OP_NAME = "update"; 
	public static final String DELETE_OP_NAME = "delete";

	public static Hashtable<String, Type> map = new Hashtable<String,Type>();
	public static Hashtable<Type, Type> reverse = new Hashtable<Type, Type>();
	
	static { 
		map.put(GET_OP_NAME, Type.GET);
		map.put(INSERT_OP_NAME, Type.INSERT);
		map.put(UPDATE_OP_NAME, Type.UPDATE);
		map.put(DELETE_OP_NAME, Type.DELETE);	

		reverse.put(Type.GET, Type.GET); 
		reverse.put(Type.INSERT, Type.DELETE); 
		reverse.put(Type.DELETE, Type.INSERT); 
		reverse.put(Type.UPDATE, Type.UPDATE);

	}
	
	/* a simplistic pattern: 
 		op_type(e, p, v, g, [v_old])\r\n
	*/
	private Type type; 
	// e, p, v, g, v_old as params
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

	public void invertVParam() { 
		String tmp = params[2]; // v 
		params[2] = params[4]; 	// v = v_old 
		params[4] = params[2]; 	// v_old = v
	}

	public String printToString() { 
		StringBuffer buf = new StringBuffer("Type " + this.type); 
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
