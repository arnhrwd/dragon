package dragon.network.messages.node.starttopo;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class StartTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 4041973859623721785L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public StartTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.START_TOPOLOGY);
		this.topologyId=topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			node.startTopology(topologyId);
			sendSuccess();
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(e.getMessage());
		}
	}

}
