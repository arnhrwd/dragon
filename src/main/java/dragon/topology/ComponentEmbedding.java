package dragon.topology;

import java.util.HashMap;

import dragon.network.NodeDescriptor;

/**
 * @author aaron
 *
 */
public class ComponentEmbedding extends HashMap<String,HashMap<Integer,NodeDescriptor>>{
	private static final long serialVersionUID = -6653485330603838000L;
	
	/**
	 * @param componentId
	 * @param taskId
	 * @param node
	 */
	public void put(String componentId, Integer taskId, NodeDescriptor node) {
		if(!containsKey(componentId)) {
			put(componentId,new HashMap<Integer,NodeDescriptor>());
		}
		HashMap<Integer,NodeDescriptor> taskMap = get(componentId);
		taskMap.put(taskId, node);
	}
	
	
	/**
	 * @return
	 */
	public ReverseComponentEmbedding getReverseComponentEmbedding() {
		ReverseComponentEmbedding rce = new ReverseComponentEmbedding();
		for(String componentId : keySet()) {
			for(Integer taskId : get(componentId).keySet()) {
				NodeDescriptor desc = get(componentId).get(taskId);
				rce.put(desc, componentId, taskId);
			}
		}
		return rce;
	}
}
