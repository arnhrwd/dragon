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
	 *
	 */
	@Override
	public void process() {
		final Node node  = Node.inst();
		final NodeContext context = node.getNodeProcessor().getContext();
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
