package dragon.network.messages.node;

import java.util.HashMap;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;

public class AcceptingJoinMessage extends NodeMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2231251843142684620L;
	public NodeDescriptor nextNode;
	public NodeContext context;
	public AcceptingJoinMessage(NodeDescriptor nextNode, NodeContext context) {
		super(NodeMessage.NodeMessageType.ACCEPTING_JOIN);
		this.nextNode=nextNode;
		this.context=context;
	}
	
}
