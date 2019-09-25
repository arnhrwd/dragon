package dragon.network.messages.node;

public class StopTopologyErrorMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6062382885436856253L;
	public String topologyId;
	public String error;
	public StopTopologyErrorMessage(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.STOP_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
