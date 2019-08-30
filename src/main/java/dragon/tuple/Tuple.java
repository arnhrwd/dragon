package dragon.tuple;

public class Tuple {
	private String sourceComponent;
	private String sourceStreamId;
	private Fields fields;
	
	public Tuple() {
		fields=new Fields();
	}
	
	public Tuple(Fields fields) {
		this.fields=fields;
	}
	
	public Tuple(Fields fields,Values values) {
		this.fields=fields;
		for(int i=0;i<values.size();i++) {
			fields.set(i, values.get(i));
		}
	}
	
	public void setValues(Values values) {
		for(int i=0;i<values.size();i++) {
			fields.set(i, values.get(i));
		}
	}
	
	public Fields getFields() {
		return fields;
	}
	
	public Object getValueByField(String fieldName) {
		return fields.get(fields.getFieldMap().get(fieldName));
	}
	
	public String getSourceComponent() {
		return sourceComponent;
	}
	
	public String getSourceStreamId() {
		return sourceStreamId;
	}

}
