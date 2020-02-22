package dragon.network.messages.node.getstatus;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeStatus;
import dragon.network.messages.node.NodeMessage;
import dragon.network.operations.GetStatusGroupOp;

/**
 * Get status of the particular daemon.
 * @author aaron
 *
 */
public class GetStatusNMsg extends NodeMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public GetStatusNMsg() {
		super(NodeMessage.NodeMessageType.GET_STATUS);
	}

	public void process() {
		final Node node = Node.inst();
		final NodeContext context  = node.getNodeProcessor().getContext();
		GetStatusGroupOp gsgo = (GetStatusGroupOp) getGroupOp();
		NodeStatus nodeStatus = node.getStatus();
		nodeStatus.context = context;
		gsgo.nodeStatus=nodeStatus;
		gsgo.sendSuccess();
	}
}
