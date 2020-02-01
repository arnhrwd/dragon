package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class HaltTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = -7506239870047998404L;
	
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
	public HaltTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.HALT_TOPOLOGY_ERROR);
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
