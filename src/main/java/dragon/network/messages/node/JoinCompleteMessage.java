package dragon.network.messages.node;

public class JoinCompleteMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -325715588776629134L;

	public JoinCompleteMessage() {
		super(NodeMessageType.JOIN_COMPLETE);
	}

}
