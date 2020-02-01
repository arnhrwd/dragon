package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class ResumeTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 2059331915207059763L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public ResumeTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
