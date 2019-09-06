package dragon.topology.base;

import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;

public class BaseRichBolt extends Bolt implements IRichBolt {
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		
	}
	
	@Override
	public void execute(Tuple tuple) {
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void ack(Object id) {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object id) {
		// TODO Auto-generated method stub
		
	}
	
}
