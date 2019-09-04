package dragon.network.messages.node;

import dragon.network.NodeDescriptor;
import dragon.network.messages.Message;

public class NodeMessage extends Message {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1656333391539097974L;

	public static enum NodeMessageType {
		JOIN_REQUEST
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
