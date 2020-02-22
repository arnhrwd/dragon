package dragon.network.messages.node.getstatus;

import dragon.network.Node;
import dragon.network.NodeStatus;
import dragon.network.messages.node.NodeMessage;
import dragon.network.operations.GetStatusGroupOp;

/**
 * Return the status of the dragon daemon
 * @author aaron
 *
 */
public class StatusNMsg extends NodeMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public final NodeStatus nodeStatus;
	
	/**
	 * 
	 */
	public StatusNMsg(NodeStatus nodeStatus) {
		super(NodeMessage.NodeMessageType.STATUS);
		this.nodeStatus=nodeStatus;
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		((GetStatusGroupOp)(node.getOpsProcessor()
				.getGroupOp(getGroupOp().getId())))
				.aggregate(nodeStatus);
		receiveSuccess();
	}

}
