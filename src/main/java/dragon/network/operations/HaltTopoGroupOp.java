package dragon.network.operations;

import dragon.network.messages.Message;
import dragon.network.messages.node.HaltTopologyErrorMessage;
import dragon.network.messages.node.HaltTopologyMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopologyHaltedMessage;

public class HaltTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 7324344914355135103L;
	private final String topologyId;
	
	public HaltTopoGroupOp(Message orig,IOpSuccess success, IOpFailure failure) {
		super(orig,success,failure);
		topologyId = ((dragon.network.messages.service.HaltTopologyMessage)orig).topologyId;
		
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new HaltTopologyMessage(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyHaltedMessage(topologyId);
	}
	
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new HaltTopologyErrorMessage(topologyId,error);
	}

}
