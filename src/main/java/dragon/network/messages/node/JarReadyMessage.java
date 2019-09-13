package dragon.network.messages.node;

public class JarReadyMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8486176138295531405L;
	public String topologyId;
	public JarReadyMessage(String topologyId) {
		super(NodeMessage.NodeMessageType.JAR_READY);
		this.topologyId = topologyId;
	}

}
