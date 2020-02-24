package dragon.network.messages.node.preparetopo;

import dragon.network.messages.node.NodeMessage;

public class TopologyIDNMsg extends NodeMessage {
	private static final long serialVersionUID = 2400002396627898858L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * 
	 * @param topologyId
	 */
	public TopologyIDNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_ID);
		this.topologyId=topologyId;
	}

}
