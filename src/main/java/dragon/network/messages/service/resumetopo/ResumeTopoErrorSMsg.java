package dragon.network.messages.service.resumetopo;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

/**
 * Error attempting to resume the topology.
 * @author aaron
 *
 */
public class ResumeTopoErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 3488028938154008168L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public ResumeTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}
}
