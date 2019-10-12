package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class TermTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 320378867671700289L;
	public final String topologyId;
	public final String error;
	public TermTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
