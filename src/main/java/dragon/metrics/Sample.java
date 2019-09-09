package dragon.metrics;

import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;
import dragon.utils.Time;

public class Sample {
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
}
