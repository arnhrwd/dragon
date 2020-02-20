package dragon.network.messages.node.stoptopo;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class TermTopoNMsg extends NodeMessage {
	private static final long serialVersionUID = -4627720827502632039L;
	
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

}
