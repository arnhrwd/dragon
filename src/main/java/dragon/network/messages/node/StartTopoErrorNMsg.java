package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class StartTopoErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = 1580653942766147873L;
	
	/**
	 * 
	 */
	public final String error;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public StartTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY_ERROR);
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
