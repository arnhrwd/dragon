package dragon.metrics;

import java.io.Serializable;

import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;
import dragon.utils.ComponentTaskBuffer;
import dragon.utils.Time;

/**
 * Sample container class, that records a sample from a given bolt or spout.
 * 
 * @author aaron
 *
 */
public class Sample implements Serializable {
	private static final long serialVersionUID = 8919792917425064566L;

	/**
	 * the time the sample was taken
	 */
	public long timestamp;

	/**
	 * 
	 */
	public int inputQueueSize;

	/**
	 * 
	 */
	public int outputQueueSize;

	/**
	 * 
	 */
	public long processed;

	/**
	 * 
	 */
	public long emitted;

	/**
	 * 
	 */
	public long transferred;

	public boolean isSpout;
	public double inputQueueRatio = 0;
	public double outputQueueRatio = 0;
	public int outputBufferSpace = 0;
	public double avgLatency = 0.0;

	/**
	 * @param bolt
	 */
	public Sample(Bolt bolt) {
		timestamp = Time.currentTimeMillis();
		inputQueueSize = bolt.getInputCollector().getQueue().size();
		inputQueueRatio = inputQueueSize / bolt.getInputCollector().getQueue().getCapacity();
		ComponentTaskBuffer ctb = bolt.getOutputCollector().getComponentTaskBuffer();
		outputQueueSize = 0;
		outputQueueRatio = 0.0;
		for (String componentId : ctb.keySet()) {
			for (String streamId : ctb.get(componentId).keySet()) {
				outputQueueSize += ctb.get(componentId).get(streamId).size();
			}
		}
		outputBufferSpace = bolt.getOutputCollector().getTotalBufferSpace();
		if (outputBufferSpace != 0)
			outputQueueRatio = outputQueueSize / bolt.getOutputCollector().getTotalBufferSpace();
		processed = bolt.getProcessed();
		emitted = bolt.getEmitted();
		transferred = bolt.getTransferred();
		avgLatency = bolt.getAvgLatency();
		isSpout = false;
	}

	/**
	 * @param spout
	 */
	public Sample(Spout spout) {
		timestamp = Time.currentTimeMillis();
		inputQueueSize = 0;
		ComponentTaskBuffer ctb = spout.getOutputCollector().getComponentTaskBuffer();
		outputQueueSize = 0;
		outputQueueRatio = 0.0;
		for (String componentId : ctb.keySet()) {
			for (String streamId : ctb.get(componentId).keySet()) {
				outputQueueSize += ctb.get(componentId).get(streamId).size();
			}
		}
		outputBufferSpace = spout.getOutputCollector().getTotalBufferSpace();
		if (outputBufferSpace != 0)
			outputQueueRatio = outputQueueSize / spout.getOutputCollector().getTotalBufferSpace();
		processed = 0;
		emitted = spout.getEmitted();
		transferred = spout.getTransferred();
		isSpout = true;
	}

	/**
	 * 
	 */
	public Sample() {
		// TODO Auto-generated constructor stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		String out = "";
		out += "timestamp=" + timestamp + "\n";
		out += "inputQueueSize=" + inputQueueSize + "\n";
		out += "outputQueueSize=" + outputQueueSize + "\n";
		out += "processed=" + processed + "\n";
		out += "emitted=" + emitted + "\n";
		out += "transferred=" + transferred + "\n";
		return out;
	}

}
