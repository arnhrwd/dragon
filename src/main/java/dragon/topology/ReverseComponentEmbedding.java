package dragon.topology;

import java.util.HashMap;

import dragon.network.NodeDescriptor;

/**
 * @author aaron
 *
 */
public class ReverseComponentEmbedding extends HashMap<NodeDescriptor,ComponentMap> {
	private static final long serialVersionUID = 6454226915002134280L;

	/**
	 * @param desc
	 * @param componentId
	 * @param taskId
	 */
	public void put(NodeDescriptor desc, String componentId, Integer taskId) {
		if(!containsKey(desc)) {
			put(desc,new ComponentMap());
		}
		ComponentMap componentMap = get(desc);
		componentMap.put(componentId, taskId);
	}
	
	/**
	 * @param desc
	 * @param componentId
	 * @param taskId
	 * @return
	 */
	public boolean contains(NodeDescriptor desc,String componentId,Integer taskId) {
		if(!containsKey(desc)) return false;
		return get(desc).contains(componentId,taskId);
	}
	
	/**
	 * @param desc
	 * @param componentId
	 * @return
	 */
	public boolean contains(NodeDescriptor desc,String componentId) {
		if(!containsKey(desc)) return false;
		return get(desc).containsKey(componentId);
	}
}
