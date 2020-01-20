package dragon.network.messages.node;

public class TopoResumedNMsg extends NodeMessage {
	private static final long serialVersionUID = -6174559101773764224L;
	public final String topologyId;
	public TopoResumedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_RESUMED);
		this.topologyId=topologyId;
	}

}
