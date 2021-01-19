package dragon.network.messages.service.termtopo;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

/**
 * Error attempting to terminate the topology.
 * @author aaron
 *
 */
public class TermTopoErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 320378867671700289L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public TermTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}

}
