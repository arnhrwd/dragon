package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class JarReadyNMsg extends NodeMessage {
	private static final long serialVersionUID = 8486176138295531405L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public JarReadyNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.JAR_READY);
		this.topologyId = topologyId;
	}

}
