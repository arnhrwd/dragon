package dragon.network.messages.service.getnodecontext;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.messages.service.ServiceMessage;

/**
 * Get the list of daemons that this daemon knows about.
 * 
 * @author aaron
 *
 */
public class GetNodeContextSMsg extends ServiceMessage {
	private static final long serialVersionUID = -640901729958656459L;

	/**
	 * 
	 */
	public GetNodeContextSMsg() {
		super(ServiceMessage.ServiceMessageType.GET_NODE_CONTEXT);
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		NodeContext nc = new NodeContext();
		nc.putAll(node.getNodeProcessor().getAliveContext());
		client(new NodeContextSMsg(nc));
	}

}
