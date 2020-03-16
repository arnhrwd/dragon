package dragon.task;

import java.util.List;
import java.util.Set;

import dragon.Constants;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Fields;

/**
 * @author aaron
 *
 */
public class TopologyContext {
	/**
	 * Always in the range [0..N-1] where
	 * N is the number of instances of component.
	 */
	private final int taskIndex;
	
	/**
	 * Always in the range [0..M-1] where
	 * M is the total number of instances in the
	 * topology.
	 */
	private final int taskId;
	
	/**
	 * 
	 */
	private final String componentId;
	
	/**
	 * Simply the list of integers [0..N-1]
	 */
	private final List<Integer> taskIndices;
	
	/**
	 * List of unique task ids for this component.
	 */
	private final List<Integer> taskIds;
	
	/**
	 * 
	 */
	private final OutputFieldsDeclarer declarer;
	
	/**
	 * @param componentId
	 * @param taskIndex
	 * @param taskId
	 * @param taskIndices
	 * @param taskIds
	 * @param declarer
	 */
	public TopologyContext(String componentId,int taskIndex,int taskId,
			List<Integer> taskIndices, List<Integer> taskIds,
			OutputFieldsDeclarer declarer) {
		this.componentId=componentId;
		this.taskIndex=taskIndex;
		this.taskId=taskId;
		this.taskIndices=taskIndices;
		this.taskIds=taskIds;
		this.declarer=declarer;
	}
	
	/**
	 * @return the list of task ids for this component
	 * which are unique over all components in the topology
	 */
	public List<Integer> getComponentTasks() {
		return taskIds;
	}
	
	/**
	 * @return the list of task indices for this component
	 * which are the numbers [0..N-1] where N is the number
	 * of instances of this component
	 */
	public List<Integer> getComponentTaskIndices() {
		return taskIndices;
	}
	
	/**
	 * @return an integer in the range [0..N-1] where N
	 * is the number of instances of this component
	 */
	public int getThisTaskIndex() {
		return taskIndex;
	}
	
	/**
	 * @param streamId
	 * @return the fields for the given stream, if it 
	 * exists, or for the direct stream if that exists,
	 * or null otherwise
	 */
	public Fields getThisOutputFields(String streamId) {
		Fields f = declarer.getFields(streamId);
		if(f!=null) return f;
		return declarer.getFieldsDirect(streamId);
	}
	
	/**
	 * Note that this returns streams declared
	 * for direct emit, as well as regular streams, and the default
	 * stream if declared for use {@link dragon.Constants.DEFAULT_STREAM},
	 * i.e. if fields are declared without any stream id.
	 * @return a list of streams that have been declared for
	 * this component
	 */
	public Set<String> getThisStreams() {
		Set<String> streams = declarer.getStreams();
		streams.remove(Constants.SYSTEM_STREAM_ID);
		return streams;
	}
	
	/**
	 * 
	 * @return a unique integer from [0..M-1] where M is
	 * the total number of instances of all components in
	 * the topology
	 */
	public int getThisTaskId() {
		return taskId;
	}
	
	/**
	 * @return the name of this component
	 */
	public String getThisComponentId() {
		return componentId;
	}
}
