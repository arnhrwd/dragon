package dragon.task;

import java.util.List;

public class TopologyContext {
	private final int taskIndex;
	private final String componentId;
	private final List<Integer> taskIds;
	
	public TopologyContext(String componentId,int taskIndex,List<Integer> taskIds) {
		this.componentId=componentId;
		this.taskIndex=taskIndex;
		this.taskIds=taskIds;
	}
	
	public List<Integer> getComponentTasks(String componentId) {
		return taskIds;
	}
	
	public int getThisTaskIndex() {
		return taskIndex;
	}
	
	public String getThisComponentId() {
		return componentId;
	}
}
