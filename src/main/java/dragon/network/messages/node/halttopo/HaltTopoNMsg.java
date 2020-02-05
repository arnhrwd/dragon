package dragon.network.messages.node.halttopo;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.NodeMessage.NodeMessageType;

/**
 * @author aaron
 *
 */
public class HaltTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 2169549008736905572L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public HaltTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.HALT_TOPOLOGY);
		this.topologyId=topologyId;
	}
}
