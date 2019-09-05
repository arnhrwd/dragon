package dragon;

import java.io.Serializable;
import java.util.HashSet;

import dragon.tuple.Tuple;

public class NetworkTask implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6164101511657361631L;
	private Tuple tuple;
	private HashSet<Integer> taskIds;
	private String componentId;
	private String topologyId;

	public NetworkTask(Tuple tuple,HashSet<Integer> taskIds,String componentId, String topologyId) {
		this.tuple=tuple;
		this.taskIds=taskIds;
		this.componentId=componentId;
	}
	
	public Tuple getTuple() {
		return tuple;
	}
	
	public HashSet<Integer> getTaskIds(){
		return taskIds;
	}
	
	public String getComponentId() {
		return componentId;
	}
	
	public String getTopologyId() {
		return topologyId;
	}

}
