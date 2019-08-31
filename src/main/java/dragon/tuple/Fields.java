package dragon.tuple;


import java.io.Serializable;
import java.util.HashMap;

public class Fields implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -134149710944581963L;
	Object[] fields;
	HashMap<String,Integer> fieldMap;
	String[] fieldNames;
	
	public Fields(String...fieldNames) {
		this.fieldNames=fieldNames;
		fieldMap = new HashMap<String,Integer>();
		fields = new Object[fieldNames.length];
		for(int i=0;i<fieldNames.length;i++) {
			fields[i]=new Object();
			fieldMap.put(fieldNames[i], i);
		}
	}
	
	public Object get(int i) {
		return fields[i];
	}
	
	public void set(int i,Object value) {
		fields[i]=value;
	}
	
	public HashMap<String,Integer> getFieldMap(){
		return fieldMap;
	}

	public Fields copy() {
		Fields f = new Fields(this.fieldNames);
		return f;
	}
	
	

}
