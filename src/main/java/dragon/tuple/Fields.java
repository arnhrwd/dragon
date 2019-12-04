package dragon.tuple;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

public class Fields implements Serializable, Cloneable {
	private static final long serialVersionUID = -134149710944581963L;
	private final Object[] values;
	private transient HashMap<String,Integer> fieldMap;  // save a little bit on network bandwidth
	private final String[] fieldNames;
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
		out.writeInt(fieldNames.length);
		for(int i=0;i<fieldNames.length;i++) {
			out.writeUTF(fieldNames[i]);
			if(values[i] instanceof String) {
				out.writeByte(0);
				out.writeUTF((String) values[i]);
			} else if(values[i] instanceof Integer) {
				out.writeByte(1);
				out.writeInt((int) values[i]);
			} else if(values[i] instanceof Long) {
				out.writeByte(2);
				out.writeLong((long) values[i]);
			} else if(values[i] instanceof Float) {
				out.writeByte(3);
				out.writeFloat((float) values[i]);
			} else if(values[i] instanceof Double) {
				out.writeByte(4);
				out.writeDouble((double) values[i]);
			} else if(values[i] instanceof Boolean) {
				out.writeByte(5);
				out.writeBoolean((boolean) values[i]);
			} else {
				out.writeByte(127);
				out.writeObject(values[i]);
			}
		}
	}
	
	public static Fields readFromStream(ObjectInputStream in) throws ClassNotFoundException, IOException {
		Integer size = in.readInt();
		String[] fieldNames = new String[size];
		Object[] values = new Object[size];
		for(int i=0;i<size;i++) {
			fieldNames[i]=in.readUTF();
			switch(in.readByte()) {
			case 0:
				values[i]=in.readUTF();
				break;
			case 1:
				values[i]=in.readInt();
				break;
			case 2:
				values[i]=in.readLong();
				break;
			case 3:
				values[i]=in.readFloat();
				break;
			case 4:
				values[i]=in.readDouble();
				break;
			case 5:
				values[i]=in.readBoolean();
				break;
			case 127:
				values[i]=in.readObject();
				break;
			}
		}
		
		Fields fields=new Fields(fieldNames);
		fields.set(values);
		return fields;
	}

}
