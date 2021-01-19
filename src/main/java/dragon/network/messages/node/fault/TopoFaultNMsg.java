package dragon.network.messages.node.fault;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * 
 * @author aaron
 *
 */
public class TopoFaultNMsg extends NodeMessage {
	private static final long serialVersionUID = 3042202836722710445L;
	private final static Logger log = LogManager.getLogger(TopoFaultNMsg.class);
	
	/**
	 * 
	 */
	final public String topologyId;
	
	/**
	 * 
	 * @param topologyId
	 */
	public TopoFaultNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_FAULT);
		this.topologyId=topologyId;
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		try {
			node.setTopologyFault(topologyId);
		} catch (DragonTopologyException e) {
			log.error(e.getMessage());
		}
	}

}
