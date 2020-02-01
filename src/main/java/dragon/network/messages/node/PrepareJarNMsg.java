package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class PrepareJarNMsg extends NodeMessage {
	private static final long serialVersionUID = -7900745704249481502L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 */
	public byte[] topologyJar;
	
	/**
	 * @param topologyName
	 * @param topologyJar
	 */
	public PrepareJarNMsg(String topologyName, byte[] topologyJar) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR);
		this.topologyId=topologyName;
		this.topologyJar=topologyJar;
	}

}
