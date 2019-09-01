package dragon.topology.base;

import java.util.Map;

import dragon.Config;
import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class IRichSpout extends Spout implements Runnable, Cloneable {
	
	public void run() {
		nextTuple();
		getLocalCluster().runComponentTask(this);
	}
	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	public void ack(Object id) {
		
	}
	
	public void fail(Object id) {
		
	}
	
	public void nextTuple() {
	
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
