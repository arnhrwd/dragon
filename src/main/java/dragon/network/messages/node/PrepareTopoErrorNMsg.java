package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

public class PrepareTopoErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = 2180031153355565198L;
	public final String topologyId;
	public final String error;
	public PrepareTopoErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

	@Override
	public String getError() {
		return error;
	}

}
