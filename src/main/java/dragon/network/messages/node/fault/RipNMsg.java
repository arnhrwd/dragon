package dragon.network.messages.node.fault;

import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * Tell a node that it has been considered dead, and it should
 * rest until it receives a context update.
 * @author aaron
 *
 */
public class RipNMsg extends NodeMessage {
	private static final long serialVersionUID = 7583554976911359154L;

	/**
	 * 
	 */
	public RipNMsg() {
		super(NodeMessage.NodeMessageType.RIP);
	}

	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		node.rip();
	}
}
