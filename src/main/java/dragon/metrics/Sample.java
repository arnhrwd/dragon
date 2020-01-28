package dragon.metrics;

import java.io.Serializable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;
import dragon.utils.ComponentTaskBuffer;
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
	public long gcTime;
	
	public Sample(Bolt bolt){
		timestamp = Time.currentTimeMillis();
		inputQueueSize = bolt.getInputCollector().getQueue().size();
		ComponentTaskBuffer ctb = bolt.getOutputCollector().getComponentTaskBuffer();
		outputQueueSize=0;
		for(String componentId:ctb.keySet()) {
			for(String streamId:ctb.get(componentId).keySet()) {
				outputQueueSize += ctb.get(componentId).get(streamId).size();
			}
		}
		processed = bolt.getProcessed();
		emitted = bolt.getEmitted();
		transferred = bolt.getTransferred();
		
	}
	
	public Sample(Spout spout){
		timestamp = Time.currentTimeMillis();
		inputQueueSize=0;
		ComponentTaskBuffer ctb = spout.getOutputCollector().getComponentTaskBuffer();
		outputQueueSize=0;
		for(String componentId:ctb.keySet()) {
			for(String streamId:ctb.get(componentId).keySet()) {
				outputQueueSize += ctb.get(componentId).get(streamId).size();
			}
		}
		processed = 0;
		emitted = spout.getEmitted();
		transferred = spout.getTransferred();
	}
	
	public Sample() {
		// TODO Auto-generated constructor stub
	}
	
	

	public String toString(){
		String out = "";
		out+="timestamp="+timestamp+"\n";
		out+="inputQueueSize="+inputQueueSize+"\n";
		out+="outputQueueSize="+outputQueueSize+"\n";
		out+="processed="+processed+"\n";
		out+="emitted="+emitted+"\n";
		out+="transferred="+transferred+"\n";
		out+="gcTime="+gcTime+"\n";
		return out;
	}
	
	
}
