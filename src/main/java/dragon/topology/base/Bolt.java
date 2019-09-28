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
	private long processed=0;
	private InputCollector inputCollector;
	
	public final void setTickTuple(Tuple tuple) {
		tickTuple=tuple;
	}
	
	@Override
	public final void run() {
		Tuple tuple;
		if(isClosing()) {
			close();
			setClosed();
			return;
		}
		if(tickTuple!=null) {
			tuple=tickTuple;
			tickTuple=null;
		} else {
			tuple = getInputCollector().getQueue().poll();
		}
		if(tuple!=null){
			getOutputCollector().resetEmit();
			execute(tuple);
			tuple.crushRecyclable(1);
			processed++;

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

	public final void setInputCollector(InputCollector inputCollector) {
		this.inputCollector = inputCollector;
	}
	
	public final InputCollector getInputCollector() {
		return inputCollector;
	}
	
	public final long getProcessed(){
		return processed;
	}
	
}
