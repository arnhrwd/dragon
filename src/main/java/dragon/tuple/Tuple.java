package dragon.tuple;

public class Tuple {
	private Fields fields;
	
	public Tuple() {
		fields=new Fields();
	}
	
	public Fields getFields() {
		return fields;
	}
	
	public Object getValueByField(String fieldName) {
		return fields.getFieldMap().get(fieldName);
	}

}
