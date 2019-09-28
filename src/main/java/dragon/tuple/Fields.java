package dragon.tuple;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

public class Fields implements Serializable, Cloneable {
	private static final long serialVersionUID = -134149710944581963L;
	private Object[] values;
	private HashMap<String,Integer> fieldMap;
	private String[] fieldNames;

	public Fields(String...fieldNames) {
		this.fieldNames=fieldNames;
		fieldMap = new HashMap<String,Integer>();
		values = new Object[fieldNames.length];
		for(int i=0;i<fieldNames.length;i++) {
			values[i] = null;
			fieldMap.put(fieldNames[i], i);
		}
	}

	public Object get(int i) {
		return values[i];
	}

	public void set(int i,Object value) {
		values[i]=value;
	}
	
	public void set(Object[] values) {
		for(int i=0;i<values.length;i++) {
			set(i,values[i]);
		}
	}

	public HashMap<String,Integer> getFieldMap(){
		return fieldMap;
	}

	public Fields copy() {
		Fields f = new Fields(this.fieldNames);
		return f;
	}

	public Object[] getValues() {
		return values;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public String getFieldNamesAsString() {
		String names="<";
		for(String name : fieldNames) {
			names+=name+",";
		}
		return names+">";
	}

	public int size(){
		return fieldNames.length;
	}
	
	public void sendToStream(ObjectOutputStream out) throws IOException {
		out.writeObject(fieldNames);
		out.writeObject(values.clone());
	}
	
	public static Fields readFromStream(ObjectInputStream in) throws ClassNotFoundException, IOException {
		String[] fieldNames = (String[]) in.readObject();
		Fields fields=new Fields(fieldNames);
		Object[] values = (Object[]) in.readObject();
		fields.set(values);
		return fields;
	}

}
