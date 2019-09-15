package dragon.network.messages.node;

public class PrepareJarMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7900745704249481502L;
	public String topologyName;
	public byte[] topologyJar;
	
	public PrepareJarMessage(String topologyName, byte[] topologyJar) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR);
		this.topologyName=topologyName;
		this.topologyJar=topologyJar;
	}

}
