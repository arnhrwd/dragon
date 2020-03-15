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
	private final List<Integer> taskIndices;
	
	/**
	 * @param componentId
	 * @param taskIndex
	 * @param taskIndices
	 */
	public TopologyContext(String componentId,int taskIndex,List<Integer> taskIndices) {
		this.componentId=componentId;
		this.taskIndex=taskIndex;
		this.taskIndices=taskIndices;
	}
	
	/**
	 * @param componentId
	 * @return
	 */
	public List<Integer> getComponentTasks(String componentId) {
		return taskIndices;
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
