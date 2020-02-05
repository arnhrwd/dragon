package dragon.network.messages.node.context;

import dragon.network.NodeContext;
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

}
