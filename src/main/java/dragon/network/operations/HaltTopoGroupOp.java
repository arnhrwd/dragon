package dragon.network.operations;

import dragon.network.messages.Message;
import dragon.network.messages.node.HaltTopoErrorNMsg;
import dragon.network.messages.node.HaltTopoNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopoHaltedNMsg;

public class HaltTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 7324344914355135103L;
	private final String topologyId;
	
	public HaltTopoGroupOp(Message orig,IOpSuccess success, IOpFailure failure) {
		super(success,failure);
		topologyId = ((dragon.network.messages.service.HaltTopoSMsg)orig).topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new HaltTopoNMsg(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoHaltedNMsg(topologyId);
	}
	
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new HaltTopoErrorNMsg(topologyId,error);
	}

}
