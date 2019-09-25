package dragon.network.messages.node;

import dragon.network.NodeDescriptor;
import dragon.network.messages.Message;

public class NodeMessage extends Message {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1656333391539097974L;

	public static enum NodeMessageType {
		JOIN_REQUEST,
		ACCEPTING_JOIN,
		JOIN_COMPLETE,
		CONTEXT_UPDATE,
		PREPARE_TOPOLOGY,
		TOPOLOGY_READY,
		START_TOPOLOGY,
		PREPARE_JAR_ERROR,
		PREPARE_JAR,
		STOP_TOPOLOGY,
		JAR_READY,
		TOPOLOGY_STARTED,
		TOPOLOGY_STOPPED,
		STOP_TOPOLOGY_ERROR,
		START_TOPOLOGY_ERROR
		
	}
	
	private NodeMessageType type;
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

	

}
