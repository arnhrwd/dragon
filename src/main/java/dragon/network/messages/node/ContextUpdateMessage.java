package dragon.network.messages.node;

import dragon.network.NodeContext;

public class ContextUpdateMessage extends NodeMessage {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 58312454197597093L;
	public NodeContext context;
	public ContextUpdateMessage(NodeContext context) {
		super(NodeMessage.NodeMessageType.CONTEXT_UPDATE);
		this.context=context;
	}

}
