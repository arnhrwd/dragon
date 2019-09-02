package dragon.topology.base;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;


public class IRichBolt extends Bolt implements Cloneable {
	private Log log = LogFactory.getLog(IRichBolt.class);
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
	
	@Deprecated
	public void ack(Object id) {
		
	}
	
	@Deprecated
	public void fail(Object id) {
		
	}
	
	public void execute(Tuple tuple) {
	
	}
	
	public void close() {
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		return conf;
	}
	
	public Object clone()throws CloneNotSupportedException{  
		return super.clone();  
	}  


}
