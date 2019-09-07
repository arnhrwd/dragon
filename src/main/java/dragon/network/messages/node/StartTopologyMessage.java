package dragon.network.messages.node;

public class StartTopologyMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4041973859623721785L;
	public String topologyId;
	public StartTopologyMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
