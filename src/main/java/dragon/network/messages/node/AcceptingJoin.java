package dragon.network.messages.node;

import dragon.network.NodeDescriptor;

public class AcceptingJoin extends NodeMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2231251843142684620L;
	public NodeDescriptor nextNode;
	public AcceptingJoin(NodeDescriptor nextNode) {
		super(NodeMessage.NodeMessageType.ACCEPTING_JOIN);
		this.nextNode=nextNode;
	}
	
}
