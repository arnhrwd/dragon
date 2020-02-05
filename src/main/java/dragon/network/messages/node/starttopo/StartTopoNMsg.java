package dragon.network.messages.node.starttopo;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.NodeMessage.NodeMessageType;

/**
 * @author aaron
 *
 */
public class StartTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 4041973859623721785L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public StartTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
