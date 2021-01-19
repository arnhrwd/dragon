package dragon.network.messages.node.resumetopo;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class ResumeTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = 2059331915207059763L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public ResumeTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			node.resumeTopology(topologyId);
			sendSuccess();
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(e.getMessage());
		}
	}

}
