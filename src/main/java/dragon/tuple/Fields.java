package dragon.tuple;


import java.util.HashMap;

public class Fields {
	Object[] fields;
	HashMap<String,Object> fieldMap;
	
	public Fields(String...fieldNames) {
		fieldMap = new HashMap<String,Object>();
		fields = new Object[fieldNames.length];
		for(int i=0;i<fieldNames.length;i++) {
			fields[i]=new Object();
			fieldMap.put(fieldNames[i], fields[i]);
		}
	}
	
	Object get(int i) {
		return fields[i];
	}
	
	HashMap<String,Object> getFieldMap(){
		return fieldMap;
	}
}
