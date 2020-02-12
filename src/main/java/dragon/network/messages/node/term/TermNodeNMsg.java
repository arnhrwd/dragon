package dragon.network.messages.node.term;

import dragon.network.messages.node.NodeMessage;

/**
 * signal the node to terminate, the jvm should quit
 * @author aaron
 *
 */
public class TermNodeNMsg extends NodeMessage {
	private static final long serialVersionUID = 8381003553368160704L;

	public TermNodeNMsg(NodeMessageType type) {
		super(NodeMessage.NodeMessageType.TERMINATE_NODE);
	}

}
