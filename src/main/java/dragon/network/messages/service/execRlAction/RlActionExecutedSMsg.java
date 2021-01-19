package dragon.network.messages.service.execRlAction;

import dragon.network.messages.service.ServiceMessage;

public class RlActionExecutedSMsg extends ServiceMessage {
	
	private static final long serialVersionUID = 7547937203268785164L;
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public RlActionExecutedSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.RL_ACTION_EXECUTED);
		this.topologyId=topologyId;
	}


}
