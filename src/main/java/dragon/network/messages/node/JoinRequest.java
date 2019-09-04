package dragon.network.messages.node;


public class JoinRequest extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4994962717012324017L;

	public JoinRequest() {
		super(NodeMessage.NodeMessageType.JOIN_REQUEST);
	}

}
