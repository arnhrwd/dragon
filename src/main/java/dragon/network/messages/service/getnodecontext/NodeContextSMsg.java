package dragon.network.messages.service.getnodecontext;

import dragon.network.NodeContext;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class NodeContextSMsg extends ServiceMessage {
	private static final long serialVersionUID = 4632062460713793529L;
	
	/**
	 * 
	 */
	public final NodeContext context;
	
	/**
	 * @param context
	 */
	public NodeContextSMsg(NodeContext context) {
		super(ServiceMessage.ServiceMessageType.NODE_CONTEXT);
		this.context=context;
	}

}
