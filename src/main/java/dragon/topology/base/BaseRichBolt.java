package dragon.topology.base;

import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;

public class BaseRichBolt extends Bolt implements IRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9187667675571919817L;

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
