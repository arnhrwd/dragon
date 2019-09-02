package dragon.topology.base;

import java.util.Map;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class BaseRichSpout extends IRichSpout {

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	@Override
	@Deprecated
	public void ack(Object id) {
		
	}
	
	@Override
	@Deprecated
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
