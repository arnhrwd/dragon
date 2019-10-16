package dragon.network.messages.service;

import dragon.network.NodeContext;

public class NodeContextSMsg extends ServiceMessage {
	private static final long serialVersionUID = 4632062460713793529L;
	public final NodeContext context;
	public NodeContextSMsg(NodeContext context) {
		super(ServiceMessage.ServiceMessageType.NODE_CONTEXT);
		this.context=context;
	}

}
