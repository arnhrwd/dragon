package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class StopTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = -4627720827502632039L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public StopTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.STOP_TOPOLOGY);
		this.topologyId = topologyId;
	}

}
