package dragon.network.messages.node;

public class TopologyResumedMessage extends NodeMessage {
	private static final long serialVersionUID = -6174559101773764224L;
	public final String topologyId;
	public TopologyResumedMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_RESUMED);
		this.topologyId=topologyId;
	}

}
