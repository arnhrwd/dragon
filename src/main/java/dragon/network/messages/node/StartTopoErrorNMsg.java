package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

public class StartTopoErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = 1580653942766147873L;
	public final String error;
	public final String topologyId;
	public StartTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

	@Override
	public String getError() {
		return error;
	}

}
