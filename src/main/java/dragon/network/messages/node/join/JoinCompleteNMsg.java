package dragon.network.messages.node.join;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
import dragon.network.Node.NodeState;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class JoinCompleteNMsg extends NodeMessage {
	private static final long serialVersionUID = -325715588776629134L;
	private final static Logger log = LogManager.getLogger(JoinCompleteNMsg.class);

	/**
	 * 
	 */
	public JoinCompleteNMsg() {
		super(NodeMessageType.JOIN_COMPLETE);
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node  = Node.inst();
		if(node.getNodeState()!=NodeState.ACCEPTING_JOIN) {
			log.error("unexpected message: "+NodeMessage.NodeMessageType.JOIN_COMPLETE.name());
		} else {
			node.setNodeState(NodeState.OPERATIONAL);
		}
	}

}
