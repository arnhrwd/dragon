package dragon.network.messages.service.listtopo;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class ListToposSMsg extends ServiceMessage {
	private static final long serialVersionUID = -8279553169106206333L;
	
	/**
	 * 
	 */
	public ListToposSMsg() {
		super(ServiceMessage.ServiceMessageType.LIST_TOPOLOGIES);
	}

}
