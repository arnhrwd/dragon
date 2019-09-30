package dragon.topology.base;

import java.io.Serializable;
import java.util.Map;

import dragon.Config;
import dragon.LocalCluster;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class Component implements Runnable, Cloneable, Serializable{
	private static final long serialVersionUID = -3296255524018955053L;
	private TopologyContext context;
	private LocalCluster localCluster;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	private Collector collector;
	private long emitted=0;
	private long transferred=0;
	private boolean closing=false;
	private boolean closed=false;
	
	public final void setClosing() {
		closing=true;
	}
	
	public final boolean isClosing() {
		return closing;
	}
	
	public final void setClosed() {
		closed=true;
	}
	
	public final boolean isClosed() {
		return closed;
	}
	
	public final void setOutputCollector(Collector collector) {
		closing=false;
		closed=false;
		emitted=0;
		transferred=0;
		this.collector=collector;
	}
	
	public final Collector getOutputCollector() {
		return collector;
	}
	
	public final void setLocalCluster(LocalCluster localCluster) {
		this.localCluster=localCluster;
	}  
	
	public final LocalCluster getLocalCluster() {
		return localCluster;
	}
	
	public final void setTopologyContext(TopologyContext context) {
		this.context=context;
	}
	
	public final String getComponentId() {
		return context.getThisComponentId();
	}
	
	public final int getTaskId() {
		return context.getThisTaskIndex();
	}
	
	public final void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
		this.outputFieldsDeclarer=declarer;
	}
	
	public final OutputFieldsDeclarer getOutputFieldsDeclarer() {
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
	
	public final void incEmitted(long inc){
		emitted+=inc;
	}
	
	public final void incTransferred(long inc){
		transferred+=inc;
	}
	
	public final long getEmitted(){
		return emitted;
	}
	
	public final long getTransferred(){
		return transferred;
	}
}
