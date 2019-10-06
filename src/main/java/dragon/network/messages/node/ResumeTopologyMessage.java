package dragon.network.messages.node;

public class ResumeTopologyMessage extends NodeMessage {
	private static final long serialVersionUID = 2059331915207059763L;
	public final String topologyId;
	public ResumeTopologyMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
