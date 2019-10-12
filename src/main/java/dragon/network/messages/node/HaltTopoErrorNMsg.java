package dragon.network.messages.node;

public class HaltTopoErrorNMsg extends NodeMessage {
	private static final long serialVersionUID = -8596472187084310338L;
	public final String topologyId;
	public final String error;
	
	public HaltTopoErrorNMsg(String topologyId, String error) {
		super(NodeMessage.NodeMessageType.HALT_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
}
