package dragon.network;

import java.util.HashMap;

import dragon.tuple.NetworkTask;
import dragon.utils.NetworkTaskBuffer;

public class ComponentQueueMap extends HashMap<String,StreamQueueMap>{
	private static final long serialVersionUID = 2633646610735734489L;
	private final int bufferSize;
	
	public ComponentQueueMap(int bufferSize){
		this.bufferSize=bufferSize;
	}
	
	public void put(NetworkTask task) throws InterruptedException {
		String componentId = task.getComponentId();
		StreamQueueMap streamQueueMap = get(componentId);
		streamQueueMap.put(task);
	}
	
	public NetworkTaskBuffer getBuffer(NetworkTask task) {
		return get(task.getComponentId()).getBuffer(task);
	}

	public void prepare(String componentId, String streamId) {
		if(!containsKey(componentId)) {
			StreamQueueMap streamQueueMap = new StreamQueueMap(bufferSize);
			streamQueueMap.prepare(streamId);
			put(componentId, streamQueueMap);
		} else {
			get(componentId).prepare(streamId);
		}
		
	}

	public void drop(String componentName, String streamId) {
		if(containsKey(componentName)) {
			get(componentName).drop(streamId);
			if(get(componentName).isEmpty()) remove(componentName);
		}
		
	}
}
