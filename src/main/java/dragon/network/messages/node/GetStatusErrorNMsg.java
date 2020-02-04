package dragon.network.messages.node;

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
