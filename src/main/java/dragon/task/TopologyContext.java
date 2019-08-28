package dragon.task;

import java.util.ArrayList;
import java.util.List;

public class TopologyContext {
	int taskIndex;
	
	public List<Integer> getComponentTasks(String componentId) {
		List<Integer> taskIds = new ArrayList<Integer>();
		return taskIds;
	}
	
	public int getThisTaskIndex() {
		return taskIndex;
	}
}
