package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class ResumeTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 3488028938154008168L;
	public final String topologyId;
	public final String error;
	public ResumeTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
