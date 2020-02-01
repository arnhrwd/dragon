package dragon.network;

import java.util.HashMap;

import dragon.tuple.NetworkTask;
import dragon.utils.NetworkTaskBuffer;

/**
 * Maintains a set of queues for components, where the queues
 * have a given bufferSize.
 * @author aaron
 *
 */
public class ComponentQueueMap extends HashMap<String,StreamQueueMap>{
	private static final long serialVersionUID = 2633646610735734489L;
	
	/**
	 * 
	 */
	private final int bufferSize;
	
	/**
	 * @param bufferSize
	 */
	public ComponentQueueMap(int bufferSize){
		this.bufferSize=bufferSize;
	}
	
	/**
	 * @param task
	 * @throws InterruptedException
	 */
	public void put(NetworkTask task) throws InterruptedException {
		String componentId = task.getComponentId();
		StreamQueueMap streamQueueMap = get(componentId);
		streamQueueMap.put(task);
	}
	
	/**
	 * @param task
	 * @return
	 */
	public NetworkTaskBuffer getBuffer(NetworkTask task) {
		return get(task.getComponentId()).getBuffer(task);
	}

	/**
	 * @param componentId
	 * @param streamId
	 */
	public void prepare(String componentId, String streamId) {
		if(!containsKey(componentId)) {
			StreamQueueMap streamQueueMap = new StreamQueueMap(bufferSize);
			streamQueueMap.prepare(streamId);
			put(componentId, streamQueueMap);
		} else {
			get(componentId).prepare(streamId);
		}
		
	}

	/**
	 * @param componentName
	 * @param streamId
	 */
	public void drop(String componentName, String streamId) {
		if(containsKey(componentName)) {
			get(componentName).drop(streamId);
			if(get(componentName).isEmpty()) remove(componentName);
		}
		
	}
}
