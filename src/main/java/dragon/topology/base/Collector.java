package dragon.topology.base;

import dragon.LocalCluster;
import dragon.NetworkTask;
import dragon.utils.DurableCircularBuffer;

public class Collector {
	protected DurableCircularBuffer<NetworkTask> outputQueue;
	protected LocalCluster localCluster;
	
	public Collector(LocalCluster localCluster,int bufSize, String persitanceDir) {
		this.localCluster=localCluster;
		outputQueue=new DurableCircularBuffer<NetworkTask>(bufSize,persitanceDir);
	}
	
	public DurableCircularBuffer<NetworkTask> getQueue(){
		return outputQueue;
	}
}
