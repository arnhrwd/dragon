package dragon.tuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Tuple implements IRecyclable, Serializable {
	@SuppressWarnings("unused")
	private final static Log log = LogFactory.getLog(Tuple.class);
	private static final long serialVersionUID = -8616313770722910200L;
	private String sourceComponent;
	private String sourceStreamId;
	private Integer sourceTaskId;
	private Fields fields;
	
	public static enum Type {
		APPLICATION,
		TERMINATE,
		FREEZE,
		CHECKPOINT,
		PRECYCLE
	}
	
	private Type type;
	
	public Tuple() {
		type=Type.APPLICATION;
		fields=new Fields();
	}
	
	public Tuple(Fields fields) {
		type=Type.APPLICATION;
		this.fields=fields.copy();
	}
	
	public Tuple(Fields fields,Values values) {
		type=Type.APPLICATION;
		this.fields=fields.copy();
		setValues(values);
	}
	
	public void setValues(Values values) {
		for(int i=0;i<values.size();i++) {
			fields.set(i, values.get(i));
		}
	}
	
	public void setType(Type type) {
		this.type=type;
	}
	
	public Type getType() {
		return type;
	}
	
	public void clearValues() {
		for(int i=0;i<fields.size();i++) {
			fields.set(i, null);
		}
	}
	
	public Object getValue(int index){
		return fields.getValues()[index];
	}
	
	public Object[] getValues(){
		return fields.getValues();
	}
	
	public Fields getFields() {
		return fields;
	}
	
	public void setFields(Fields fields) {
		this.fields=fields;
	}
	
	public Object getValueByField(String fieldName) {
		return fields.get(fields.getFieldMap().get(fieldName));
	}
	
	public void setSourceComponent(String componentId) {
		this.sourceComponent=componentId;
	}
	
	public void setSourceStreamId(String streamId) {
		this.sourceStreamId=streamId;
	}
	
	public void setSourceTaskId(Integer taskId) {
		this.sourceTaskId = taskId;
	}
	
	public String getSourceComponent() {
		return sourceComponent;
	}
	
	public String getSourceStreamId() {
		return sourceStreamId;
	}
	
	public Integer getSourceTaskId() {
		return sourceTaskId;
	}
	
	@Override
	public String toString() {
		return "source("+sourceComponent+":"+sourceStreamId+":"+sourceTaskId+")<"+fields.getValues().toString()+">";
	}

	@Override
	public void recycle() {
		clearValues();
		sourceComponent=null;
		sourceStreamId=null;
		sourceTaskId=null;
		type=Tuple.Type.APPLICATION;
	}

	@Override
	public IRecyclable newRecyclable() {
		return new Tuple(this.fields);
	}
	
	public void sendToStream(ObjectOutputStream out) throws IOException {
		out.writeUTF(sourceComponent);
		out.writeUTF(sourceStreamId);
		out.writeInt(sourceTaskId);
		out.writeUTF(type.name());
		fields.sendToStream(out);
	}
	
	public static Tuple readFromStream(ObjectInputStream in) throws ClassNotFoundException, IOException {
		String sourceComponent = in.readUTF();
		String sourceStreamId = in.readUTF();
		Integer sourceTaskId = in.readInt();
		Type type = Type.valueOf(in.readUTF());
		Fields fields = Fields.readFromStream(in);
		Tuple t = RecycleStation.getInstance().getTupleRecycler(fields.getFieldNamesAsString()).newObject();
		t.setSourceComponent(sourceComponent);
		t.setSourceStreamId(sourceStreamId);
		t.setSourceTaskId(sourceTaskId);
		t.setFields(fields);
		t.setType(type);
		return t;
	}

}
