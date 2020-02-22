package dragon.network.messages.node.removetopo;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TopoRemovedNMsg extends NodeMessage {
	private static final long serialVersionUID = -2724894142332243034L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoRemovedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_REMOVED);
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
