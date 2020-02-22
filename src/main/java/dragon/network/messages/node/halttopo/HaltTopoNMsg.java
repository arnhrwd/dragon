package dragon.network.messages.node.halttopo;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class HaltTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 2169549008736905572L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public HaltTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.HALT_TOPOLOGY);
		this.topologyId=topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			node.haltTopology(topologyId);
			sendSuccess();
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(e.getMessage());
		}
	}
}
