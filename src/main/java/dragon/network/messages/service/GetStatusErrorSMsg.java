package dragon.network.messages.service;

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
