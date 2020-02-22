package dragon.network.messages.node.fault;

import dragon.network.Node;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;

/**
 * 
 * @author aaron
 *
 */
public class NodeFaultNMsg extends NodeMessage {
	private static final long serialVersionUID = 6245532944874405972L;

	/**
	 * 
	 */
	public final NodeDescriptor desc;
	
	/**
	 * 
	 * @param desc
	 */
	public NodeFaultNMsg(NodeDescriptor desc) {
		super(NodeMessage.NodeMessageType.NODE_FAULT);
		this.desc=desc;
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		node.removeNode(desc);
	}

}
