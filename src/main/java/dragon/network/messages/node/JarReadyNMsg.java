package dragon.network.messages.node;

public class JarReadyNMsg extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8486176138295531405L;
	public String topologyId;
	public JarReadyNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.JAR_READY);
		this.topologyId = topologyId;
	}

}
