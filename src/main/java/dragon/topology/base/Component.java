package dragon.topology.base;

import java.util.Map;

import dragon.Config;
import dragon.LocalCluster;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class Component implements Runnable, Cloneable{
	private TopologyContext context;
	private LocalCluster localCluster;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	private Collector collector;
	
	public void setOutputCollector(Collector collector) {
		this.collector=collector;
	}
	
	public Collector getOutputCollector() {
		return collector;
	}
	
	public void setLocalCluster(LocalCluster localCluster) {
		this.localCluster=localCluster;
	}  
	
	public LocalCluster getLocalCluster() {
		return localCluster;
	}
	
	public void setTopologyContext(TopologyContext context) {
		this.context=context;
	}
	
	public String getComponentId() {
		return context.getThisComponentId();
	}
	
	public int getTaskId() {
		return context.getThisTaskIndex();
	}
	
	public void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
		this.outputFieldsDeclarer=declarer;
	}
	
	public OutputFieldsDeclarer getOutputFieldsDeclarer() {
		return outputFieldsDeclarer;
	}

	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		return conf;
	}
	
	public Object clone()throws CloneNotSupportedException{  
		return super.clone();  
	}  
}
