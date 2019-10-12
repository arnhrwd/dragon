package dragon.network.messages.service;

public class ResumeTopoErrorSMsg extends ServiceMessage {
	private static final long serialVersionUID = 3488028938154008168L;
	public final String topologyId;
	public final String error;
	public ResumeTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
