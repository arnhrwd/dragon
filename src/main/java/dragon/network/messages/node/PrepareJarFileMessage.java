package dragon.network.messages.node;

public class PrepareJarFileMessage extends NodeMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7900745704249481502L;
	public String topologyName;
	public byte[] topologyJar;
	
	public PrepareJarFileMessage(String topologyName, byte[] topologyJar) {
		super(NodeMessage.NodeMessageType.PREPARE_JARFILE);
		this.topologyName=topologyName;
		this.topologyJar=topologyJar;
	}

}
