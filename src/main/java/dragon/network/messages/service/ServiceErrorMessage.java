package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

/**
 * Generic service error message.
 * @author aaron
 *
 */
public class ServiceErrorMessage extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 1L;
	/**
	 * Error message to show the user.
	 */
	public final String error;
	
	/**
	 * 
	 * @param type
	 * @param error
	 */
	public ServiceErrorMessage(ServiceMessageType type,String error) {
		super(type);
		this.error=error;
	}

	@Override
	public String getError() {
		return error;
	}

}
