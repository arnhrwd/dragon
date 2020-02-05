package dragon.network.messages.node.preparetopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.NodeMessage.NodeMessageType;

/**
 * Error attempting to prepare a topology for running.
 * @author aaron
 *
 */
public class PrepareTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = 2180031153355565198L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public PrepareTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}

}
