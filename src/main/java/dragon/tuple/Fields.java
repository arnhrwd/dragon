package dragon.tuple;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

public class Fields implements Serializable, Cloneable {
	private static final long serialVersionUID = -134149710944581963L;
	private Object[] values;
	private transient HashMap<String,Integer> fieldMap;  // save a little bit on network bandwidth
	private String[] fieldNames;
	private transient String name; // save a little bit more

	public Fields(String...fieldNames) {
		this.fieldNames=fieldNames;
		fieldMap = new HashMap<String,Integer>(fieldNames.length);
		values = new Object[fieldNames.length];
		for(int i=0;i<fieldNames.length;i++) {
			values[i] = null;
			fieldMap.put(fieldNames[i], i);
		}
		name=buildName();
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
		name=buildName();
		return f;
	}

	public Object[] getValues() {
		return values;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}
	
	private String buildName() {
		String names="<";
		for(String name : fieldNames) {
			names+=name+",";
		}
		return names+">";
	}

	public String getFieldNamesAsString() {
		return name;
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
