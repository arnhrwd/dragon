package dragon.topology;

import java.util.HashMap;

import dragon.network.NodeDescriptor;

public class ComponentEmbedding extends HashMap<String,HashMap<Integer,NodeDescriptor>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6653485330603838000L;

	public void put(String componentId, Integer taskId, NodeDescriptor node) {
		if(!containsKey(componentId)) {
			put(componentId,new HashMap<Integer,NodeDescriptor>());
		}
		HashMap<Integer,NodeDescriptor> taskMap = get(componentId);
		taskMap.put(taskId, node);
	}
	
}
