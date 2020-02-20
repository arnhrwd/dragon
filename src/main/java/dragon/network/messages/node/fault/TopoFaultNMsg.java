package dragon.network.messages.node.fault;

import dragon.network.messages.node.NodeMessage;

/**
 * 
 * @author aaron
 *
 */
public class TopoFaultNMsg extends NodeMessage {
	private static final long serialVersionUID = 3042202836722710445L;
	
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

}
