package dragon.topology;

import java.util.HashMap;

public class ComponentMap extends HashMap<String,TaskSet>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1993314788597117756L;

	public void put(String componentId,Integer taskId) {
		if(!containsKey(componentId)) {
			put(componentId,new TaskSet());
		}
		TaskSet taskIds = get(componentId);
		taskIds.add(taskId);
	}

	public boolean contains(String componentId, Integer taskId) {
		if(!containsKey(componentId)) return false;
		return get(componentId).contains(taskId);
	}
}
