package dragon.topology;

import dragon.tuple.Fields;

public class OutputFieldsDeclarer {
	public Fields fields;
	
	public void declare(Fields fields) {
		this.fields=fields;
	}
	
	public void declare(boolean direct,Fields fields) {
		
	}
	
	public void declare(String streamId,Fields fields) {
		
	}
}
