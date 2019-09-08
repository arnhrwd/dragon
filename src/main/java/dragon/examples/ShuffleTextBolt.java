package dragon.examples;

import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Fields;
import dragon.tuple.Tuple;
import dragon.tuple.Values;

public class ShuffleTextBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6564807375331163333L;
	OutputCollector collector;
	int myId;
	int seen=0;
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		myId=context.getThisTaskIndex();
	}
	public void execute(Tuple tuple) {
		//System.out.println("shuffeBolt["+myId+"] received "+tuple.getValueByField("number"));
		seen++;
		//System.out.println("shuffeBolt["+myId+"] seen "+seen);
		
		collector.emit("uuid",new Values(tuple.getFields().getValues()));
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare("uuid",new Fields("uuid"));
	}
}
