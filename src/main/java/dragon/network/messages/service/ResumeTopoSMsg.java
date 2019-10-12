package dragon.network.messages.service;

public class ResumeTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4132366816670626369L;
	public final String topologyId;
	public ResumeTopoSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
