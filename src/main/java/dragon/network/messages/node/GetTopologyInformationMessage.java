package dragon.network.messages.node;

public class GetTopologyInformationMessage extends NodeMessage {
	private static final long serialVersionUID = 1383319162954166063L;
	public final String topologyId;
	public GetTopologyInformationMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.GET_TOPOLOGY_INFORMATION);
		this.topologyId=topologyId;
	}
}
