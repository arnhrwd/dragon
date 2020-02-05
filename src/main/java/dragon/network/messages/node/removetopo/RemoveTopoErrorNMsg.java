package dragon.network.messages.node.removetopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * Error attempting to remove a topology.
 * @author aaron
 *
 */
public class RemoveTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = -8217846248840488597L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public RemoveTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.REMOVE_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}
}
