package dragon.examples;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Tuple;

import java.util.HashSet;
import java.util.Map;

public class FieldsBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7640696004039861046L;
	private int myId;
	private int uniqueNumbers;
	private HashSet<Integer> numbers;

	public FieldsBolt(int uniqueNumbers) {
		this.uniqueNumbers = uniqueNumbers;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		myId=context.getThisTaskIndex();
		numbers=new HashSet<Integer>();
	}
	
	public void execute(Tuple tuple) {
		Integer number = (Integer)tuple.getValueByField("number");
		if (numbers.add(number)) {
			System.out.println("receiverBolt[" + myId + "] received " + tuple.getValueByField("number"));
		}
		if(numbers.size()>uniqueNumbers){
			System.out.println("ERROR");
		}
	}
}
