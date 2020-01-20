package dragon.network.messages.node;

public class TopoStoppedNMsg extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2556748215092282932L;
	public String topologyId;
	public TopoStoppedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_STOPPED);
		this.topologyId = topologyId;
	}

}
