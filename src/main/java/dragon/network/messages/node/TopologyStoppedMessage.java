package dragon.network.messages.node;

public class TopologyStoppedMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2556748215092282932L;
	public String topologyId;
	public TopologyStoppedMessage(String topoologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_STOPPED);
		this.topologyId = topologyId;
	}

}
