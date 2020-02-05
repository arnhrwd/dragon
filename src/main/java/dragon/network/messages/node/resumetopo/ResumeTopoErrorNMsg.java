package dragon.network.messages.node.resumetopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class ResumeTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = 6682111392028265462L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public ResumeTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.RESUME_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}
}
