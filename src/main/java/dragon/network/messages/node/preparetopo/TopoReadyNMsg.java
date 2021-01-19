package dragon.network.messages.node.preparetopo;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TopoReadyNMsg extends NodeMessage {
	private static final long serialVersionUID = -1794256917559159190L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoReadyNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_READY);
		this.topologyId=topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		receiveSuccess();
	}

}
