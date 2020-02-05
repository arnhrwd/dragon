package dragon.network.messages.service.resumetopo;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class ResumeTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4132366816670626369L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public ResumeTopoSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
