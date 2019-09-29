package dragon.examples;

import java.util.Map;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichSpout;
import dragon.tuple.Fields;
import dragon.tuple.Values;

public class NumberSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5807899269206807053L;
	SpoutOutputCollector collector;
	int num=0;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
	}
	
	@Override
	public void nextTuple() {
		if(num<10000000) {
			//System.out.println("emitting "+num);
			collector.emit(new Values(num));
			num++;
		} else if(num==10000000) {
			System.out.println("finished emitting");
			num++;
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}
}
