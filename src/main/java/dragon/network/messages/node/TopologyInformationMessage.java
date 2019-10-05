package dragon.network.messages.node;

public class TopologyInformationMessage extends NodeMessage {
	private static final long serialVersionUID = 4785147438021153895L;
	public final String topologyId;
	public TopologyInformationMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_INFORMATION);
		this.topologyId=topologyId;
	}

}
