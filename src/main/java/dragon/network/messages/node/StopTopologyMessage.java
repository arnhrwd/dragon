package dragon.network.messages.node;

public class StopTopologyMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4627720827502632039L;
	public String topologyId;
	public StopTopologyMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.STOP_TOPOLOGY);
		this.topologyId = topologyId;
	}

}
