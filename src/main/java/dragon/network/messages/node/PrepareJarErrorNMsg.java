package dragon.network.messages.node;

public class PrepareJarErrorNMsg extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2722133277354980722L;
	public String topologyId;
	public String error;
	public PrepareJarErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
