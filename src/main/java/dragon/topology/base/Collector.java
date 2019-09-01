package dragon.topology.base;

import dragon.LocalCluster;
import dragon.NetworkTask;
import dragon.utils.CircularBuffer;


public class Collector {
	protected CircularBuffer<NetworkTask> outputQueue;
	protected LocalCluster localCluster;
	public Object lock = new Object();
	
	public Collector(LocalCluster localCluster,int bufSize) {
		this.localCluster=localCluster;
		outputQueue=new CircularBuffer<NetworkTask>(bufSize);
	}
	
	public CircularBuffer<NetworkTask> getQueue(){
		return outputQueue;
	}
}
