package dragon.network.messages.service;

public class RunFailedMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9155772452009152465L;
	public String topologyId;
	public String error;
	public RunFailedMessage(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RUN_FAILED);
		this.topologyId=topologyId;
		this.error=error;
	}

}
