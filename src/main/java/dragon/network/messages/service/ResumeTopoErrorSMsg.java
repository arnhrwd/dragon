package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class ResumeTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 3488028938154008168L;
	
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
	public ResumeTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY_ERROR);
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
