package dragon.network.messages.node.stoptopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * Error attempting to stop a topology.
 * @author aaron
 *
 */
public class TermTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = -6062382885436856253L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public TermTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.TERMINATE_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		receiveError();
	}
}
