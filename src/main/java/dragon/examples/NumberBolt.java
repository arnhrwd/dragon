package dragon.examples;

import java.util.HashSet;
import java.util.Map;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Tuple;

public class NumberBolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3957233181035456948L;
	HashSet<Integer> numbers;
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		numbers=new HashSet<Integer>();
	}
	
	public void execute(Tuple tuple) {
		if(tuple.getSourceStreamId().equals("odd")||tuple.getSourceStreamId().equals("even")) {
			Integer number = (Integer)tuple.getValueByField("number");
			if(numbers.contains(number)) {
				System.out.println("ERROR");
				System.exit(-1);
			}
			//System.out.println("received "+number+" from task id "+tuple.getSourceTaskId());
			numbers.add(number);
			System.out.println("received "+numbers.size()+" numbers");
		} else {
			String uuid = (String)tuple.getValueByField("uuid");
			System.out.println("recieved "+uuid);
		}
	}
}
