package dragon.network.messages.service.getnodecontext;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class GetNodeContextSMsg extends ServiceMessage {
	private static final long serialVersionUID = -640901729958656459L;

	/**
	 * 
	 */
	public GetNodeContextSMsg() {
		super(ServiceMessage.ServiceMessageType.GET_NODE_CONTEXT);
	}

}
