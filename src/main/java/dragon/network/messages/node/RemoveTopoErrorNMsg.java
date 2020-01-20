package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

public class RemoveTopoErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = -8217846248840488597L;
	public final String topologyId;
	public final String error;
	public RemoveTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.REMOVE_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
