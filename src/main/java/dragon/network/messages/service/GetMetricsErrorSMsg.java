package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class GetMetricsErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 779753438805275630L;
	
	/**
	 * 
	 */
	public final String error;
	
	/**
	 * @param error
	 */
	public GetMetricsErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.GET_METRICS_ERROR);
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
