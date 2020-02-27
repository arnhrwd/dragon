package dragon.topology.base;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Config;
import dragon.LocalCluster;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

/**
 * @author aaron
 *
 */
public class Component implements Cloneable, Serializable{
	private static final long serialVersionUID = -3296255524018955053L;
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(Component.class);
	
	/**
	 * 
	 */
	private TopologyContext context;
	
	/**
	 * 
	 */
	private LocalCluster localCluster;
	
	/**
	 * 
	 */
	private OutputFieldsDeclarer outputFieldsDeclarer;
	
	/**
	 * 
	 */
	private Collector collector;
	
	/**
	 * 
	 */
	private Long emitted=0L;
	
	/**
	 * 
	 */
	private Long transferred=0L;
	
	/**
	 * 
	 */
	protected volatile boolean closing=false;
	
	/**
	 * 
	 */
	protected volatile boolean closed=false;
	
	/**
	 * 
	 */
	public  volatile ReentrantLock lock;
	
	/**
	 * 
	 */
	public final void setClosing() {
		closing=true;
	}
	
	/**
	 * @return
	 */
	public final boolean isClosing() {
		return closing;
	}
	
	/**
	 * 
	 */
	public final void setClosed() {
		closed=true;	
	}
	
	/**
	 * @return
	 */
	public final boolean isClosed() {
		return closed;
	}
	
	/**
	 * @param collector
	 */
	public final void setOutputCollector(Collector collector) {
		closing=false;
		closed=false;
		emitted=0L;
		transferred=0L;
		this.collector=collector;
		this.lock=new ReentrantLock();
	}
	
	/**
	 * @return
	 */
	public final Collector getOutputCollector() {
		return collector;
	}
	
	/**
	 * @param localCluster
	 */
	public final void setLocalCluster(LocalCluster localCluster) {
		this.localCluster=localCluster;
	}  
	
	/**
	 * @return
	 */
	public final LocalCluster getLocalCluster() {
		return localCluster;
	}
	
	/**
	 * @param context
	 */
	public final void setTopologyContext(TopologyContext context) {
		this.context=context;
	}
	
	/**
	 * @return
	 */
	public final String getComponentId() {
		return context.getThisComponentId();
	}
	
	/**
	 * @return
	 */
	public final int getTaskId() {
		return context.getThisTaskIndex();
	}
	
	/**
	 * @return
	 */
	public final String getInstanceId() {
		return getComponentId()+":"+getTaskId();
	}
	
	/**
	 * @param declarer
	 */
	public final void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
		this.outputFieldsDeclarer=declarer;
	}
	
	/**
	 * @return
	 */
	public final OutputFieldsDeclarer getOutputFieldsDeclarer() {
		return outputFieldsDeclarer;
	}

	/**
	 * 
	 */
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * @return
	 */
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		return conf;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public Object clone()throws CloneNotSupportedException{  
		return super.clone();  
	}  
	
	/**
	 * @param inc
	 */
	public final void incEmitted(long inc){
		
			emitted+=inc;
		
	}
	
	/**
	 * @param inc
	 */
	public final void incTransferred(long inc){
		
			transferred+=inc;
		
	}
	
	/**
	 * @return
	 */
	public final long getEmitted(){
		
			return emitted;
		
	}
	
	/**
	 * @return
	 */
	public final long getTransferred(){
		
			return transferred;
		
	}
}
