package dragon.examples;

import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Tuple;

public class NumberBolt extends BaseRichBolt {
	private static final Log log = LogFactory.getLog(NumberBolt.class);
	private static final long serialVersionUID = -3957233181035456948L;
	HashSet<Integer> numbers;
	HashSet<String> text;
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		numbers=new HashSet<Integer>();
		text=new HashSet<String>();
	}
	
	public void execute(Tuple tuple) {
		//log.debug("executing tuple "+tuple);
		if(tuple.getSourceStreamId().equals("odd")||tuple.getSourceStreamId().equals("even")) {
			Integer number = (Integer)tuple.getValueByField("number");
			if(numbers.contains(number)) {
				log.error("ERROR received twice: "+number);
				System.exit(-1);
			}
			//System.out.println("received "+number+" from task id "+tuple.getSourceTaskId());
			numbers.add(number);
			if(numbers.size()==10000000)
				log.info("received "+numbers.size()+" numbers");
		} else {
			String uuid = (String)tuple.getValueByField("uuid");
			if(text==null) {
				System.out.println("text is null");
				System.exit(-1);
			}
			if(uuid==null) {
				System.out.println("uuid is null");
				System.exit(-1);
			}
			text.add(uuid);
			if(text.size()==10000000)
				log.info("recieved "+text.size()+" uuids");
		}
	}
}
