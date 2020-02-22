package dragon.network.messages.node.removetopo;

import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class RemoveTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = -4476085259913087385L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * 
	 */
	public final boolean purge;
	
	/**
	 * @param topologyId
	 */
	public RemoveTopoNMsg(String topologyId,boolean purge) {
		super(NodeMessage.NodeMessageType.REMOVE_TOPOLOGY);
		this.topologyId=topologyId;
		this.purge=purge;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			node.removeTopo(topologyId,purge);
			sendSuccess();
		} catch (DragonTopologyException e) {
			sendError(e.getMessage());
		}
	}

}
