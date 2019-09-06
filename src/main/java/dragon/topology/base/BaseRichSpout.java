package dragon.topology.base;

import java.util.Map;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class BaseRichSpout extends Spout implements IRichSpout {

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	
	public void ack(Object id) {
		
	}
	
	public void fail(Object id) {
		
	}
	
	@Override
	public void nextTuple() {
	
	}
	
	@Override
	public void close() {
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
