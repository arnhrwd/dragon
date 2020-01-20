package dragon.network.messages.node;

public class TopoHaltedNMsg extends NodeMessage {
	private static final long serialVersionUID = -5015748029777135034L;
	public final String topologyId;
	public TopoHaltedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_HALTED);
		this.topologyId=topologyId;
	}
}
