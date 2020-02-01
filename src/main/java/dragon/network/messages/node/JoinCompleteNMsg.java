package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class JoinCompleteNMsg extends NodeMessage {
	private static final long serialVersionUID = -325715588776629134L;

	/**
	 * 
	 */
	public JoinCompleteNMsg() {
		super(NodeMessageType.JOIN_COMPLETE);
	}

}
