package dragon.topology.base;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;


public class IRichBolt extends Bolt implements Runnable, Cloneable {
	private Log log = LogFactory.getLog(IRichBolt.class);
	
	public void run() {
		Tuple tuple = getInputCollector().getQueue().poll();
		if(tuple!=null){
			execute(tuple);
			getLocalCluster().runComponentTask(this);
		} else {
			getLocalCluster().runComponentTask(this);
		}
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		
	}
	
	public void ack(Object id) {
		
	}
	
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
