package dragon.network;

import java.util.HashMap;

import dragon.NetworkTask;
import dragon.utils.NetworkTaskBuffer;


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
		streamQueueMap.put(task);
	}
	
	public NetworkTaskBuffer getBuffer(NetworkTask task) {
		return get(task.getTopologyId()).getBuffer(task);
	}

	public void prepare(String topologyId, String streamId) {
		if(!containsKey(topologyId)) {
			StreamQueueMap streamQueueMap = new StreamQueueMap(bufferSize);
			streamQueueMap.prepare(streamId);
			put(topologyId, streamQueueMap);
		} else {
			get(topologyId).prepare(streamId);
		}
		
	}

	public void drop(String topologyName, String streamId) {
		if(containsKey(topologyName)) {
			get(topologyName).drop(streamId);
			if(get(topologyName).isEmpty()) remove(topologyName);
		}
		
	}
}
