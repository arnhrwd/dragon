package dragon.network.messages.node.join;

import dragon.network.Node;
import dragon.network.Node.NodeState;
import dragon.network.NodeContext;
import dragon.network.messages.node.NodeMessage;
import dragon.network.operations.JoinGroupOp;

/**
 * @author aaron
 *
 */
public class JoinRequestNMsg extends NodeMessage {
	private static final long serialVersionUID = 4994962717012324017L;

	/**
	 * 
	 */
	public JoinRequestNMsg() {
		super(NodeMessage.NodeMessageType.JOIN_REQUEST);
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		final NodeContext context = node.getNodeProcessor().getContext();
		node.setNodeState(NodeState.ACCEPTING_JOIN);
		context.put(getSender());
		JoinGroupOp jgo = (JoinGroupOp) getGroupOp();
		jgo.context=context;
		sendSuccess();
	}

}
