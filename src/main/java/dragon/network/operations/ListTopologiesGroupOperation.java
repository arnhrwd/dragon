package dragon.network.operations;

import dragon.network.messages.Message;
import dragon.network.messages.node.GetTopologyInformationErrorMessage;
import dragon.network.messages.node.GetTopologyInformationMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopologyInformationMessage;

public class ListTopologiesGroupOperation extends GroupOperation {
	private static final long serialVersionUID = 7346932652353465012L;

	public ListTopologiesGroupOperation(Message orig) {
		super(orig);
		// TODO Auto-generated constructor stub
	}

	public NodeMessage initiateNodeMessage() {
		return new GetTopologyInformationMessage("");
	}
	
	public NodeMessage successNodeMessage() {
		return new TopologyInformationMessage("");
	}
	
	public NodeMessage errorNodeMessage() {
		return new GetTopologyInformationErrorMessage("");
	}
	
}
