package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class RunTopoErrorSMsg extends ServiceMessage implements IErrorMessage {
	private static final long serialVersionUID = 9155772452009152465L;
	public final String topologyId;
	public final String error;
	public RunTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
