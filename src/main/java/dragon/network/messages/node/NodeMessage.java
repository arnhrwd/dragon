package dragon.network.messages.node;

import dragon.network.NodeDescriptor;
import dragon.network.messages.Message;
import dragon.network.operations.GroupOp;

/**
 * Node messages are sent only between nodes (daemons), and processed by the 
 * Node processor.
 * @author aaron
 *
 */
public class NodeMessage extends Message {
	private static final long serialVersionUID = -1656333391539097974L;

	public static enum NodeMessageType {
		JOIN_REQUEST,
		ACCEPTING_JOIN,
		JOIN_COMPLETE,
		CONTEXT_UPDATE,
		PREPARE_TOPOLOGY,
		TOPOLOGY_READY,
		PREPARE_TOPOLOGY_ERROR,
		START_TOPOLOGY,
		PREPARE_JAR_ERROR,
		PREPARE_JAR,
		STOP_TOPOLOGY,
		JAR_READY,
		TOPOLOGY_STARTED,
		TOPOLOGY_STOPPED,
		STOP_TOPOLOGY_ERROR,
		START_TOPOLOGY_ERROR,
		HALT_TOPOLOGY,
		GET_TOPOLOGY_INFORMATION,
		TOPOLOGY_INFORMATION,
		TOPOLOGY_HALTED,
		HALT_TOPOLOGY_ERROR,
		RESUME_TOPOLOGY,
		TOPOLOGY_RESUMED,
		RESUME_TOPOLOGY_ERROR,
		REMOVE_TOPOLOGY,
		TOPOLOGY_REMOVED,
		REMOVE_TOPOLOGY_ERROR,
		ALLOCATE_PARTITION,
		PARTITION_ALLOCATED,
		ALLOCATE_PARTITION_ERROR
	}
	
	private final NodeMessageType type;
	
	/**
	 * Used when the message is part of a group operation. Messages may or may
	 * not be part of a group operation.
	 */
	private GroupOp groupOperation;
	
	/**
	 * Used either to respond, or to complete group operations.
	 */
	private NodeDescriptor sender;
	
	public NodeMessage(NodeMessageType type) {
		this.type=type;
	}
	
	public NodeMessageType getType() {
		return type;
	}
	
	public void setSender(NodeDescriptor sender) {
		this.sender=sender;
	}
	
	public NodeDescriptor getSender() {
		return sender;
	}

	public void setGroupOp(GroupOp groupOperation) {
		this.groupOperation=groupOperation;
	}
	
	public GroupOp getGroupOp() {
		return this.groupOperation;
	}

}
