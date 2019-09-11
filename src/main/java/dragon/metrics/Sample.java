package dragon.metrics;

import java.io.Serializable;

import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;
import dragon.utils.Time;

public class Sample implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8919792917425064566L;
	public long timestamp;
	public int inputQueueSize;
	public int outputQueueSize;
	public long processed;
	public long emitted;
	public long transferred;
	
	public Sample(Bolt bolt){
		timestamp = Time.currentTimeMillis();
		inputQueueSize = bolt.getInputCollector().getQueue().getNumElements();
		outputQueueSize = bolt.getOutputCollector().getQueue().getNumElements();
		processed = bolt.getProcessed();
		emitted = bolt.getEmitted();
		transferred = bolt.getTransferred();
		
	}
	
	public Sample(Spout spout){
		timestamp = Time.currentTimeMillis();
		inputQueueSize=0;
		outputQueueSize = spout.getOutputCollector().getQueue().getNumElements();
		processed = 0;
		emitted = spout.getEmitted();
		transferred = spout.getTransferred();
	}
	
	public String toString(){
		String out = "";
		out+="timestamp="+timestamp+"\n";
		out+="inputQueueSize="+inputQueueSize+"\n";
		out+="outputQueueSize="+outputQueueSize+"\n";
		out+="processed="+processed+"\n";
		out+="emitted="+emitted+"\n";
		out+="transferred="+transferred+"\n";
		return out;
	}
}
