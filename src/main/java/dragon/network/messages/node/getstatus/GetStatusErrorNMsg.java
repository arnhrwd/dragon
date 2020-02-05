package dragon.network.messages.node.getstatus;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.NodeMessage.NodeMessageType;

/**
 * Error attempting to get the status of the daemon
 * @author aaron
 *
 */
public class GetStatusErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param error
	 */
	public GetStatusErrorNMsg(String error) {
		super(NodeMessage.NodeMessageType.GET_STATUS_ERROR, error);
	}

}
