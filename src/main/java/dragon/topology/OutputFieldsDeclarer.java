package dragon.topology;

import java.util.HashMap;

import dragon.tuple.Fields;

public class OutputFieldsDeclarer {
	public Fields fields;
	public HashMap<String,Fields> streamFields;
	
	public OutputFieldsDeclarer() {
		streamFields = new HashMap<String,Fields>();
	}
	
	public void declare(Fields fields) {
		this.fields=fields;
	}
	
	public void declare(boolean direct,Fields fields) {
		this.fields=fields;
	}
	
	public void declare(String streamId,Fields fields) {
		streamFields.put(streamId, fields);
	}
}
