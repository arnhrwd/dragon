package dragon.network.messages.node.stoptopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * Error attempting to stop a topology.
 * @author aaron
 *
 */
public class StopTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = -6062382885436856253L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public StopTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.STOP_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}
}
