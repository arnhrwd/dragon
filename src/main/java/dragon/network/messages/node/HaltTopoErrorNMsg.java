package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class HaltTopoErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = -8596472187084310338L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * 
	 */
	public final String error;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public HaltTopoErrorNMsg(String topologyId, String error) {
		super(NodeMessage.NodeMessageType.HALT_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

	/* (non-Javadoc)
	 * @see dragon.network.messages.IErrorMessage#getError()
	 */
	@Override
	public String getError() {
		return error;
	}
}
