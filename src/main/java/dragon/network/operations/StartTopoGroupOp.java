package dragon.network.operations;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StartTopoErrorNMsg;
import dragon.network.messages.node.StartTopoNMsg;
import dragon.network.messages.node.TopoStartedNMsg;


public class StartTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 2635749611866470029L;
	private final String topologyId;
	public StartTopoGroupOp(String topologyId,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.topologyId=topologyId;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new StartTopoNMsg(topologyId);
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoStartedNMsg(topologyId);
	}
	
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new StartTopoErrorNMsg(topologyId,error);
	}

}