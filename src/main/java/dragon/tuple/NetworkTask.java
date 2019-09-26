package dragon.tuple;

import java.io.Serializable;
import java.util.HashSet;

public class NetworkTask implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6164101511657361631L;
	private final Tuple tuple;
	private final HashSet<Integer> taskIds;
	private final String componentId;
	private final String topologyId;

	public NetworkTask(Tuple tuple,HashSet<Integer> taskIds,String componentId, String topologyId) {
		this.tuple=tuple;
		this.taskIds=taskIds;
		this.componentId=componentId;
		this.topologyId=topologyId;
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

	@Override
	public String toString() {
		return tuple.toString();
	}
}
