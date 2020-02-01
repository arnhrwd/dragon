package dragon.topology.base;

import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;

/**
 * @author aaron
 *
 */
public class BaseRichBolt extends Bolt implements IRichBolt {
	private static final long serialVersionUID = 9187667675571919817L;

	/* (non-Javadoc)
	 * @see dragon.topology.base.Bolt#prepare(java.util.Map, dragon.task.TopologyContext, dragon.task.OutputCollector)
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.Bolt#execute(dragon.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple tuple) {
		
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.Bolt#declareOutputFields(dragon.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	/* (non-Javadoc)
	 * @see dragon.topology.base.IRichBolt#ack(java.lang.Object)
	 */
	public void ack(Object id) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see dragon.topology.base.IRichBolt#fail(java.lang.Object)
	 */
	public void fail(Object id) {
		// TODO Auto-generated method stub
		
	}
	
}
