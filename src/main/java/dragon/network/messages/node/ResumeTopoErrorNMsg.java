package dragon.network.messages.node;

public class ResumeTopoErrorNMsg extends NodeMessage {
	private static final long serialVersionUID = 6682111392028265462L;
	public final String topologyId;
	public final String error;
	public ResumeTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.RESUME_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
