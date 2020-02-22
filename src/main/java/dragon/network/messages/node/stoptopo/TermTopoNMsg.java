package dragon.network.messages.node.stoptopo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TermTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = -4627720827502632039L;
	private final static Logger log = LogManager.getLogger(TermTopoNMsg.class);
	
	/**
	 * 
	 */
	public String topologyId;

	
	/**
	 * @param topologyId
	 */
	public TermTopoNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TERMINATE_TOPOLOGY);
		this.topologyId = topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			// starts a thread to stop the topology
			log.debug("asking node to stop the topology ["+topologyId+"]");
			node.terminateTopology(topologyId,getGroupOp());
		} catch (DragonTopologyException | DragonInvalidStateException e) {
			sendError(e.getMessage());
		} 
	}

}
