package dragon.network.messages.node.join;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
import dragon.network.Node.NodeState;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class AcceptingJoinNMsg extends NodeMessage {
	private static final long serialVersionUID = 2231251843142684620L;
	private final static Logger log = LogManager.getLogger(AcceptingJoinNMsg.class);
	
	
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
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		if(node.getNodeState()!=NodeState.JOIN_REQUESTED) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.ACCEPTING_JOIN.name());
		} else {
			receiveSuccess();
		}
	}
	
}
