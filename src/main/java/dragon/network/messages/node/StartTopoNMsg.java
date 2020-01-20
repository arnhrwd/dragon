package dragon.network.messages.node;

public class StartTopoNMsg extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4041973859623721785L;
	public String topologyId;
	public StartTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
