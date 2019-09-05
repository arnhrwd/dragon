package dragon.network.messages.node;


public class JoinRequestMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4994962717012324017L;

	public JoinRequestMessage() {
		super(NodeMessage.NodeMessageType.JOIN_REQUEST);
	}

}
