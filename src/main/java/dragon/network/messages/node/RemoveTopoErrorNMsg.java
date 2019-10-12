package dragon.network.messages.node;

public class RemoveTopoErrorNMsg extends NodeMessage {
	private static final long serialVersionUID = -8217846248840488597L;
	public final String topologyId;
	public final String error;
	public RemoveTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.REMOVE_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
