package dragon.network.messages.node.context;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class ContextUpdateNMsg extends NodeMessage {
	private static final long serialVersionUID = 58312454197597093L;
	
	/**
	 * 
	 */
	public NodeContext context;
	
	/**
	 * @param context
	 */
	public ContextUpdateNMsg(NodeContext context) {
		super(NodeMessage.NodeMessageType.CONTEXT_UPDATE);
		this.context=context;
	}
	
	/**
	 * This is an expensive, perfect reconciliation approach, to be replaced with 
	 * something more scalable in the future.
	 * 
	 * The node receives this context. If there is a key in the nodes's context
	 * that is not in the this context, then send the node's context
	 * back to the sender. In either case, put all of this context 
	 * into the node's context; prior to sending the context back to the sender
	 * if required. This way the context of both nodes becomes equal and exchanges
	 * will terminate when they are.
	 */
	@Override
	public void process() {
		final Node node  = Node.inst();
		final NodeContext context = node.getNodeProcessor().getAliveContext();
		// this is the only message that will revive a node from the dead
		node.getNodeProcessor().setAlive(getSender());
		boolean hit=false;
		for(String key : context.keySet()) {
			if(!this.context.containsKey(key)) {
				context.putAll(this.context);
				try {
					node.getComms().sendNodeMsg(this.getSender(), new ContextUpdateNMsg(context));
				} catch (DragonCommsException e) {
					node.nodeFault(getSender());
				}
				hit=true;
				break;
			}
		}
		if(!hit) context.putAll(this.context);
	}

}
