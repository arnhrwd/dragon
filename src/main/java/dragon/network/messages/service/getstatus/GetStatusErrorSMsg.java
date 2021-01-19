package dragon.network.messages.service.getstatus;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

/**
 * Error when getting status.
 * @author aaron
 *
 */
public class GetStatusErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 * @param error
	 */
	public GetStatusErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.GET_STATUS_ERROR,error);
	}
}
