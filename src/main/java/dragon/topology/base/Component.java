package dragon.topology.base;

import dragon.LocalCluster;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class Component {
	private TopologyContext context;
	private LocalCluster localCluster;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	
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
}
