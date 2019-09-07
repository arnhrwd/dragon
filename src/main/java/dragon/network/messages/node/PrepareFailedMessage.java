package dragon.network.messages.node;

public class PrepareFailedMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2722133277354980722L;
	public String topologyId;
	public String error;
	public PrepareFailedMessage(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_FAILED);
		this.topologyId=topologyId;
		this.error=error;
	}

}
