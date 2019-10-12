package dragon.network.operations;

import dragon.network.messages.Message;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.ResumeTopologyErrorMessage;
import dragon.network.messages.node.ResumeTopologyMessage;
import dragon.network.messages.node.TopologyResumedMessage;

public class ResumeTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -8685556477082460093L;
	private final String topologyId;
	public ResumeTopoGroupOp(Message orig,IOpSuccess success,IOpFailure failure) {
		super(orig,success,failure);
		topologyId = ((dragon.network.messages.service.ResumeTopologyMessage)orig).topologyId;
		
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new ResumeTopologyMessage(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyResumedMessage(topologyId);
	}
	
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new ResumeTopologyErrorMessage(topologyId,error);
	}
}
