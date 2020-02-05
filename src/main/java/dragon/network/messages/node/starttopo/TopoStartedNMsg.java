package dragon.network.messages.node.starttopo;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TopoStartedNMsg extends NodeMessage {
	private static final long serialVersionUID = 1152969383180182192L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoStartedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_STARTED);
		this.topologyId=topologyId;
	}

}
