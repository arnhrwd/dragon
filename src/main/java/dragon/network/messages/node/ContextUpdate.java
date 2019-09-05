package dragon.network.messages.node;

import dragon.network.NodeContext;

public class ContextUpdate extends NodeMessage {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 58312454197597093L;
	public NodeContext context;
	public ContextUpdate(NodeContext context) {
		super(NodeMessage.NodeMessageType.CONTEXT_UPDATE);
		this.context=context;
	}

}
