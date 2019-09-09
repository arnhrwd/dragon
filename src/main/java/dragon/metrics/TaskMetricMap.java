package dragon.metrics;

import java.util.ArrayList;
import java.util.HashMap;

public class TaskMetricMap extends HashMap<Integer,ArrayList<Sample>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1514387987198642265L;

	public void put(Integer taskId, Sample sample) {
		if(!containsKey(taskId)){
			put(taskId,new ArrayList<Sample>());
		}
		get(taskId).add(sample);
		
	}

}
