package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class TopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 599535561764460099L;
	
	/**
	 * 
	 */
	public final String topologyName;
	
	/**
	 * 
	 */
	public final String error;
	
	/**
	 * @param name
	 * @param error
	 */
	public TopoErrorSMsg(String name, String error) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_ERROR);
		this.topologyName=name;
		this.error = error;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.messages.IErrorMessage#getError()
	 */
	@Override
	public String getError() {
		return error;
	}
	
}
