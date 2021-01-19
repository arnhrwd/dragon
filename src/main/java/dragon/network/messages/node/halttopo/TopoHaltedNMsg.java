package dragon.network.messages.node.halttopo;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TopoHaltedNMsg extends NodeMessage {
	private static final long serialVersionUID = -5015748029777135034L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoHaltedNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_HALTED);
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
