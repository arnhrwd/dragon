package dragon.network.messages.node;


public class JoinRequestNMsg extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4994962717012324017L;

	public JoinRequestNMsg() {
		super(NodeMessage.NodeMessageType.JOIN_REQUEST);
	}

}
