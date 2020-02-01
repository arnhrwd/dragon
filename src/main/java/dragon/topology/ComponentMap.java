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
	 * @param taskId
	 */
	public void put(String componentId,Integer taskId) {
		if(!containsKey(componentId)) {
			put(componentId,new TaskSet());
		}
		TaskSet taskIds = get(componentId);
		taskIds.add(taskId);
	}

	/**
	 * @param componentId
	 * @param taskId
	 * @return
	 */
	public boolean contains(String componentId, Integer taskId) {
		if(!containsKey(componentId)) return false;
		return get(componentId).contains(taskId);
	}
}
