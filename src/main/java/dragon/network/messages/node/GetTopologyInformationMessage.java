package dragon.network.messages.node;

public class GetTopologyInformationMessage extends NodeMessage {
	private static final long serialVersionUID = 1383319162954166063L;
	public GetTopologyInformationMessage() {
		super(NodeMessage.NodeMessageType.GET_TOPOLOGY_INFORMATION);
	}
}
