package dragon.network.messages.node;

public class TerminateRouterErrorMessage extends NodeMessage {
	private static final long serialVersionUID = -8217846248840488597L;
	public final String topologyId;
	public final String error;
	public TerminateRouterErrorMessage(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.TERMINATE_ROUTER_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
