package dragon.topology.base;

import java.util.Map;

import dragon.Config;
import dragon.LocalCluster;
import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class IRichSpout implements Runnable, Cloneable {
	private TopologyContext context;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	private LocalCluster localCluster;
	
	private enum NEXTACTION {
		nextTuple,
		close
	};
	
	private NEXTACTION nextAction;
	
	public IRichSpout() {
		nextAction=NEXTACTION.nextTuple;
	}
	
	public void run() {
		switch(nextAction){
		case nextTuple:
			nextTuple();
			localCluster.runComponentTask(this);
			break;
		case close:
			close();
		}
		
	}
	
	public void setLocalCluster(LocalCluster localCluster) {
		this.localCluster=localCluster;
		
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
