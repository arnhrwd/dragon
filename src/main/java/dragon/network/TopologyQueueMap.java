package dragon.network;

import java.util.HashMap;

import dragon.tuple.NetworkTask;
import dragon.utils.NetworkTaskBuffer;

/**
 * Store queues for topologies.
 * @author aaron
 *
 */
public class TopologyQueueMap extends HashMap<String,ComponentQueueMap> {
	private static final long serialVersionUID = 2633646610735734489L;
	
	/**
	 * 
	 */
	private final int bufferSize;
	
	/**
	 * @param bufferSize
	 */
	public TopologyQueueMap(int bufferSize){
		this.bufferSize=bufferSize;
	}
	
	/**
	 * @param task
	 * @throws InterruptedException
	 */
	public void put(NetworkTask task) throws InterruptedException {
		String topologyId = task.getTopologyId();
		ComponentQueueMap componentQueueMap = get(topologyId);
		componentQueueMap.put(task);
	}
	
	/**
	 * @param task
	 * @return
	 */
	public NetworkTaskBuffer getBuffer(NetworkTask task) {
		return get(task.getTopologyId()).getBuffer(task);
	}

	/**
	 * @param topologyId
	 * @param componentId
	 * @param streamId
	 */
	public void prepare(String topologyId, String componentId,String streamId) {
		if(!containsKey(topologyId)) {
			ComponentQueueMap componentQueueMap = new ComponentQueueMap(bufferSize);
			componentQueueMap.prepare(componentId,streamId);
			put(topologyId, componentQueueMap);
		} else {
			get(topologyId).prepare(componentId,streamId);
		}
		
	}

	/**
	 * @param topologyId
	 * @param componentId
	 * @param streamId
	 */
	public void drop(String topologyId, String componentId,String streamId) {
		if(containsKey(topologyId)) {
			get(topologyId).drop(componentId,streamId);
			if(get(topologyId).isEmpty()) remove(topologyId);
		}
		
	}
}
