package dragon.metrics;


import java.util.HashMap;

public class ComponentMetricMap extends HashMap<String,TaskMetricMap>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8719152068849752492L;

	public void put(String componentId, Integer taskId, Sample sample) {
		if(!containsKey(componentId)){
			put(componentId,new TaskMetricMap());
		}
		get(componentId).put(taskId, sample);
	}

}
