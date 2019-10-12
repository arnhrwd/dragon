package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class HaltTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = -7506239870047998404L;
	public final String topologyId;
	public final String error;
	public HaltTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.HALT_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
