package dragon.network.messages.node;

public class TopologyStartedMessage extends NodeMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1152969383180182192L;
	public String topologyId;
	public TopologyStartedMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_STARTED);
		this.topologyId=topologyId;
	}

}
