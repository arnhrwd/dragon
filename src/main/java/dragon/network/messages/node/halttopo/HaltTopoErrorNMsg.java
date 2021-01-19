package dragon.network.messages.node.halttopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * Error attempting to halt the topology.
 * @author aaron
 *
 */
public class HaltTopoErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = -8596472187084310338L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public HaltTopoErrorNMsg(String topologyId, String error) {
		super(NodeMessage.NodeMessageType.HALT_TOPOLOGY_ERROR,error);
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
