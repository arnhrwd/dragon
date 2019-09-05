package dragon.network.messages.service;

import dragon.network.NodeContext;

public class NodeContextMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4632062460713793529L;
	public NodeContext context;
	public NodeContextMessage(NodeContext context) {
		super(ServiceMessage.ServiceMessageType.NODE_CONTEXT);
		this.context=context;
	}

}
