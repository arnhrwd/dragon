package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class PrepareJarErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = -2722133277354980722L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public PrepareJarErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR_ERROR,error);
		this.topologyId=topologyId;
	}

}
