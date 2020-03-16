package dragon.topology;

import java.util.HashMap;

import dragon.network.NodeDescriptor;

/**
 * component id ->
 *  task index ->
 *   node
 * @author aaron
 *
 */
public class ComponentEmbedding extends HashMap<String,HashMap<Integer,NodeDescriptor>>{
	private static final long serialVersionUID = -6653485330603838000L;
	
	/**
	 * @param componentId
	 * @param taskIndex
	 * @param node
	 */
	public void put(String componentId, Integer taskIndex, NodeDescriptor node) {
		if(!containsKey(componentId)) {
			put(componentId,new HashMap<Integer,NodeDescriptor>());
		}
		HashMap<Integer,NodeDescriptor> taskMap = get(componentId);
		taskMap.put(taskIndex, node);
	}
	
	
	/**
	 * @return
	 */
	public ReverseComponentEmbedding getReverseComponentEmbedding() {
		ReverseComponentEmbedding rce = new ReverseComponentEmbedding();
		for(String componentId : keySet()) {
			for(Integer taskIndex : get(componentId).keySet()) {
				NodeDescriptor desc = get(componentId).get(taskIndex);
				rce.put(desc, componentId, taskIndex);
			}
		}
		return rce;
	}
}
