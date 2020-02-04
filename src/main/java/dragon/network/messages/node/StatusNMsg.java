package dragon.network.messages.node;

import dragon.network.NodeStatus;

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

}
