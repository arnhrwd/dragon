package dragon.network.messages.node;

public class TopoRemovedNMsg extends NodeMessage {
	private static final long serialVersionUID = -2724894142332243034L;
	public final String topologyId;
	public TopoRemovedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_REMOVED);
		this.topologyId=topologyId;
	}
	
}
