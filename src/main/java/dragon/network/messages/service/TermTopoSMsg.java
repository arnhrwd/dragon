package dragon.network.messages.service;

public class TermTopoSMsg extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7620075913134267391L;
	public String topologyId;
	public TermTopoSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY);
		this.topologyId = topologyId;
	}

}
