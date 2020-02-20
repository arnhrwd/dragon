package dragon.network.messages.service.termtopo;

import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class TermTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = -7620075913134267391L;

	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 */
	public boolean purge;

	/**
	 * @param topologyId
	 */
	public TermTopoSMsg(String topologyId,boolean purge) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY);
		this.topologyId = topologyId;
		this.purge=purge;
	}

}
