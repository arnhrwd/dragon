package dragon.network.messages.service;

public class ResumeTopologyMessage extends ServiceMessage {
	private static final long serialVersionUID = -4132366816670626369L;
	public final String topologyId;
	public ResumeTopologyMessage(String topologyId) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
