package dragon.examples;

import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Tuple;

public class NumberBolt extends BaseRichBolt {
	
	int expected=0;
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		
	}
	
	public void execute(Tuple tuple) {
		System.out.println("received number: "+(Integer)tuple.getValueByField("number"));
		if(expected!=(Integer)tuple.getValueByField("number")) {
			System.exit(-1);
		}
		expected++;
	}
}
