package dragon.network.messages.node.join;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.NodeMessage.NodeMessageType;

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
