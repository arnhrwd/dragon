package dragon.topology.base;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;

public class Bolt extends Component {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6696004781292813419L;
	private Log log = LogFactory.getLog(Bolt.class);
	private Tuple tickTuple=null;
	
	public void setTickTuple(Tuple tuple) {
		tickTuple=tuple;
	}
	
	@Override
	public void run() {
		Tuple tuple;
		if(tickTuple!=null) {
			tuple=tickTuple;
			tickTuple=null;
		} else {
			tuple = getInputCollector().getQueue().poll();
		}
		if(tuple!=null){
			getOutputCollector().resetEmit();
			execute(tuple);

		} else {
			log.error("nothing on the queue!");
		}
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		
	}
	
	public void execute(Tuple tuple){
		
	}
	
	public void close() {
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	
	private InputCollector inputCollector;
	
	public void setInputCollector(InputCollector inputCollector) {
		this.inputCollector = inputCollector;
	}
	
	public InputCollector getInputCollector() {
		return inputCollector;
	}
	
	
	
}
