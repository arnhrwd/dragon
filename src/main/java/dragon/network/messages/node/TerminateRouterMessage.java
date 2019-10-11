package dragon.network.messages.node;

public class TerminateRouterMessage extends NodeMessage {
	private static final long serialVersionUID = -4476085259913087385L;
	public final String topologyId;
	public TerminateRouterMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.TERMINATE_ROUTER);
		this.topologyId=topologyId;
	}

}
