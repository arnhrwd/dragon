package dragon.network.messages.service.getstatus;

import java.util.ArrayList;

import dragon.network.NodeStatus;
import dragon.network.messages.service.ServiceMessage;

/**
 * Status information about the dragon daemons.
 * @author aaron
 *
 */
public class StatusSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1L;
	
	/**
	 * 
	 */
	public final ArrayList<NodeStatus> dragonStatus;
	
	/**
	 * 
	 */
	public StatusSMsg(ArrayList<NodeStatus> dragonStatus) {
		super(ServiceMessage.ServiceMessageType.STATUS);
		this.dragonStatus=dragonStatus;
	}

}
