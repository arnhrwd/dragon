package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

public class StopTopoErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = -6062382885436856253L;
	public final String topologyId;
	public final String error;
	public StopTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.STOP_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
