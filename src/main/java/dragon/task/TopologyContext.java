package dragon.task;

import java.util.List;

/**
 * @author aaron
 *
 */
public class TopologyContext {
	/**
	 * 
	 */
	private final int taskIndex;
	/**
	 * 
	 */
	private final String componentId;
	/**
	 * 
	 */
	private final List<Integer> taskIds;
	
	/**
	 * @param componentId
	 * @param taskIndex
	 * @param taskIds
	 */
	public TopologyContext(String componentId,int taskIndex,List<Integer> taskIds) {
		this.componentId=componentId;
		this.taskIndex=taskIndex;
		this.taskIds=taskIds;
	}
	
	/**
	 * @param componentId
	 * @return
	 */
	public List<Integer> getComponentTasks(String componentId) {
		return taskIds;
	}
	
	/**
	 * @return
	 */
	public int getThisTaskIndex() {
		return taskIndex;
	}
	
	/**
	 * @return
	 */
	public String getThisComponentId() {
		return componentId;
	}
}
