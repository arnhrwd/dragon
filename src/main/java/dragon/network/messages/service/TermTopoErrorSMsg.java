package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class TermTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 320378867671700289L;
	
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
	public TermTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY_ERROR);
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
