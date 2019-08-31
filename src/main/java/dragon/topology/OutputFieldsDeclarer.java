package dragon.topology;

import java.util.HashMap;

import dragon.Constants;
import dragon.tuple.Fields;

public class OutputFieldsDeclarer {
	public Fields fields;
	public HashMap<String,Fields> streamFields;
	
	public OutputFieldsDeclarer() {
		streamFields = new HashMap<String,Fields>();
	}
	
	public void declare(Fields fields) {
		declare(Constants.DEFAULT_STREAM,fields);
	}
	
	public void declare(boolean direct,Fields fields) {
		declare(Constants.DEFAULT_STREAM,fields);
	}
	
	public void declare(String streamId,Fields fields) {
		streamFields.put(streamId, fields);
	}
	
	public Fields getFields(String streamId) {
		return streamFields.get(streamId);
	}
}
