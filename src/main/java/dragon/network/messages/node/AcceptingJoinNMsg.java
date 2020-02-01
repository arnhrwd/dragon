package dragon.network.messages.node;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;

/**
 * @author aaron
 *
 */
public class AcceptingJoinNMsg extends NodeMessage {
	private static final long serialVersionUID = 2231251843142684620L;
	
	/**
	 * 
	 */
	public NodeDescriptor nextNode;
	
	/**
	 * 
	 */
	public NodeContext context;
	
	/**
	 * @param nextNode
	 * @param context
	 */
	public AcceptingJoinNMsg(NodeDescriptor nextNode, NodeContext context) {
		super(NodeMessage.NodeMessageType.ACCEPTING_JOIN);
		this.nextNode=nextNode;
		this.context=context;
	}
	
}
