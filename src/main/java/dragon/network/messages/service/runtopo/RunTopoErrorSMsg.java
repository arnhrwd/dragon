package dragon.network.messages.service.runtopo;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * Error attempting to run a topology.
 * @author aaron
 *
 */
public class RunTopoErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 9155772452009152465L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public RunTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}

}
