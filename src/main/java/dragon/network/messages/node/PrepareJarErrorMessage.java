package dragon.network.messages.node;

public class PrepareJarErrorMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2722133277354980722L;
	public String topologyId;
	public String error;
	public PrepareJarErrorMessage(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
