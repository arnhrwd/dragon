package dragon.network.messages.service.getstatus;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

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
