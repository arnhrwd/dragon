package dragon.tuple;

import java.io.Serializable;

public class Tuple implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8616313770722910200L;
	private String sourceComponent;
	private String sourceStreamId;
	private Integer sourceTaskId;
	private Fields fields;
	
	public Tuple() {
		fields=new Fields();
	}
	
	public Tuple(Fields fields) {
		this.fields=fields.copy();
	}
	
	public Tuple(Fields fields,Values values) {
		this.fields=fields.copy();
		for(int i=0;i<values.size();i++) {
			this.fields.set(i, values.get(i));
		}
	}
	
	public void setValues(Values values) {
		for(int i=0;i<values.size();i++) {
			fields.set(i, values.get(i));
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

}
