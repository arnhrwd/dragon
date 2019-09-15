package dragon.network.messages.service;

public class TerminateTopologyErrorMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 320378867671700289L;
	public String topologyId;
	public String error;
	public TerminateTopologyErrorMessage(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
