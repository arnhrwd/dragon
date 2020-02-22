package dragon.network.messages.node.stoptopo;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TopoTermdNMsg extends NodeMessage {
	private static final long serialVersionUID = 2556748215092282932L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoTermdNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.TOPOLOGY_TERMINATED);
		this.topologyId = topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		receiveSuccess();
	}

}
