package dragon.network.messages.service;

/**
 * Get the status of the dragon daemons
 * @author aaron
 *
 */
public class GetStatusSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1L;

	public GetStatusSMsg() {
		super(ServiceMessage.ServiceMessageType.GET_STATUS);
	}

}
