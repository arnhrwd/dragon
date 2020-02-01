package dragon.examples;

import java.util.Map;
import java.util.UUID;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichSpout;
import dragon.tuple.Fields;
import dragon.tuple.Values;

public class TextSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7656062835877209004L;
	
	/**
	 * 
	 */
	SpoutOutputCollector collector;
	
	/**
	 * 
	 */
	int num=0;
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.BaseRichSpout#open(java.util.Map, dragon.task.TopologyContext, dragon.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.BaseRichSpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();
		if(num<10000000) {
			//System.out.println("emitting "+num);
			collector.emit(new Values(randomUUIDString));
			num++;
		} else if(num==10000000) {
			System.out.println("finished emitting");
			num++;
		}
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.base.BaseRichSpout#declareOutputFields(dragon.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid"));
	}
}
