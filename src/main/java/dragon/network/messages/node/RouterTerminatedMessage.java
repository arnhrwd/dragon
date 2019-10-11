package dragon.network.messages.node;

public class RouterTerminatedMessage extends NodeMessage {
	private static final long serialVersionUID = -2724894142332243034L;
	public final String topologyId;
	public RouterTerminatedMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.ROUTER_TERMINATED);
		this.topologyId=topologyId;
	}
	
}
