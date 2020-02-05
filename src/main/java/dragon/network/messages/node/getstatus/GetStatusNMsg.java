package dragon.network.messages.node.getstatus;

import dragon.network.messages.node.NodeMessage;

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

}
