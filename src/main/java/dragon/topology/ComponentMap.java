package dragon.topology;

import java.util.HashMap;

/**
 * @author aaron
 *
 */
public class ComponentMap extends HashMap<String,TaskSet>{
	private static final long serialVersionUID = 1993314788597117756L;

	/**
	 * @param componentId
	 * @param taskIndex
	 */
	public void put(String componentId,Integer taskIndex) {
		if(!containsKey(componentId)) {
			put(componentId,new TaskSet());
		}
		TaskSet taskIndices = get(componentId);
		taskIndices.add(taskIndex);
	}

	/**
	 * @param componentId
	 * @param taskIndex
	 * @return
	 */
	public boolean contains(String componentId, Integer taskIndex) {
		if(!containsKey(componentId)) return false;
		return get(componentId).contains(taskIndex);
	}
}
