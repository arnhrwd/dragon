package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

public class PrepareJarErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = -2722133277354980722L;
	public final String topologyId;
	public final String error;
	public PrepareJarErrorNMsg(String topologyId,String error) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
