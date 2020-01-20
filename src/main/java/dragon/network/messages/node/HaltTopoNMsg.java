package dragon.network.messages.node;

public class HaltTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 2169549008736905572L;
	public final String topologyId;
	public HaltTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.HALT_TOPOLOGY);
		this.topologyId=topologyId;
	}
}
