package dragon.network.messages.node;

/**
 * Error attempting to start (run) a topology.
 * @author aaron
 *
 */
public class StartTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = 1580653942766147873L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public StartTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}

}
