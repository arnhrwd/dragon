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
	 * @param taskIndex
	 */
	public void put(NodeDescriptor desc, String componentId, Integer taskIndex) {
		if(!containsKey(desc)) {
			put(desc,new ComponentMap());
		}
		ComponentMap componentMap = get(desc);
		componentMap.put(componentId, taskIndex);
	}
	
	/**
	 * @param desc
	 * @param componentId
	 * @param taskIndex
	 * @return
	 */
	public boolean contains(NodeDescriptor desc,String componentId,Integer taskIndex) {
		if(!containsKey(desc)) return false;
		return get(desc).contains(componentId,taskIndex);
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
