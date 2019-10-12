package dragon.network.messages.node;

public class StopTopoErrorNMsg extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6062382885436856253L;
	public String topologyId;
	public String error;
	public StopTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.STOP_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
