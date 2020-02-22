package dragon.network.messages.node.starttopo;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

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
	
	/**
	 *
	 */
	@Override
	public void process() {
		receiveError();
	}

}
