package dragon.network;

import java.util.HashMap;

import dragon.NetworkTask;
import dragon.utils.CircularBuffer;

public class TopologyQueueMap extends HashMap<String,StreamQueueMap> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2633646610735734489L;
	private int bufferSize;
	
	public TopologyQueueMap(int bufferSize){
		this.bufferSize=bufferSize;
	}
	
	public void put(NetworkTask task) throws InterruptedException {
		String topologyId = task.getTopologyId();
		StreamQueueMap streamQueueMap = get(topologyId);
		if(streamQueueMap==null){
			streamQueueMap = new StreamQueueMap(bufferSize);
			put(topologyId,streamQueueMap);
		}
		streamQueueMap.put(task);
	}
	
	public CircularBuffer<NetworkTask> getBuffer(NetworkTask task) {
		return get(task.getTopologyId()).getBuffer(task);
	}
}
