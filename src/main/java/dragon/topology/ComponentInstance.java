package dragon.topology;

import java.io.Serializable;

/**
 * 
 * @author aaron
 *
 */
public class ComponentInstance implements Serializable {
	private static final long serialVersionUID = -227354838523947000L;

	/**
	 * 
	 */
	public String componentId;
	
	/**
	 * 
	 */
	public Integer taskIndex;
	
	/**
	 * 
	 * @param componentId
	 * @param taskIndex
	 */
	public ComponentInstance(String componentId,Integer taskIndex){
		this.componentId=componentId;
		this.taskIndex=taskIndex;
	}
}
