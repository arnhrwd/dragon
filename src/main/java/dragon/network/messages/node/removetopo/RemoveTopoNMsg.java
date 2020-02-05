package dragon.network.messages.node.removetopo;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.NodeMessage.NodeMessageType;

/**
 * @author aaron
 *
 */
public class RemoveTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = -4476085259913087385L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public RemoveTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.REMOVE_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
