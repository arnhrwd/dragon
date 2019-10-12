package dragon.network.messages.service;

public class RunTopoErrorSMsg extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9155772452009152465L;
	public String topologyId;
	public String error;
	public RunTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
