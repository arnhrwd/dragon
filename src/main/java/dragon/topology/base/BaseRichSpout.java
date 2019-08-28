package dragon.topology.base;

import java.util.Map;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;

public class BaseRichSpout extends IRichSpout {

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	public void ack(Object id) {
		
	}
	
	public void fail(Object id) {
		
	}
	
	public void nextTuple() {
	
	}
	
	public void close() {
		
	}
}
