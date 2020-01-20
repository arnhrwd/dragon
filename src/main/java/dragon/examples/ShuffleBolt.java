package dragon.examples;

import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Fields;
import dragon.tuple.Tuple;
import dragon.tuple.Values;

public class ShuffleBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1931282622170822462L;
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
		if(tuple.getValueByField("number")==null) {
			System.out.println("incorrect field");
			System.exit(-1);
		}
		if(((Integer)tuple.getValueByField("number"))%2==0){
			collector.emit("even",new Values(tuple.getFields().getValues()));
		} else {
			collector.emit("odd",new Values(tuple.getFields().getValues()));
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare("even",new Fields("number"));
		declarer.declare("odd",new Fields("number"));
	}
}
