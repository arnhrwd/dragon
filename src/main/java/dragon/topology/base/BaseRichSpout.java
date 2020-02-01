package dragon.topology.base;

import java.util.Map;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

/**
 * @author aaron
 *
 */
public class BaseRichSpout extends Spout implements IRichSpout {
	private static final long serialVersionUID = -4721210469464274871L;

	/* (non-Javadoc)
	 * @see dragon.topology.base.Spout#open(java.util.Map, dragon.task.TopologyContext, dragon.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.IRichSpout#ack(java.lang.Object)
	 */
	public void ack(Object id) {
		
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.IRichSpout#fail(java.lang.Object)
	 */
	public void fail(Object id) {
		
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.Spout#nextTuple()
	 */
	@Override
	public void nextTuple() {
	
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.Spout#close()
	 */
	@Override
	public void close() {
		
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.Spout#declareOutputFields(dragon.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
