package dragon.network.messages.node;

public class GetTopologyInformationErrorMessage extends NodeMessage {		
	private static final long serialVersionUID = 5026489145967008245L;
	public final String topologyId;
	public GetTopologyInformationErrorMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.GET_TOPOLOGY_INFORMATION_ERROR);
		this.topologyId=topologyId;
	}
}
