package dragon.network.messages.service.listtopo;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

/**
 * 
 * @author aaron
 *
 */
public class ListToposErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 211567004102164442L;

	/**
	 * 
	 * @param error
	 */
	public ListToposErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.LIST_TOPOLOGIES_ERROR, error);
	}

}
