package dragon.tuple;


import java.util.HashMap;

public class Fields {
	Object[] fields;
	HashMap<String,Integer> fieldMap;
	
	public Fields(String...fieldNames) {
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
}
