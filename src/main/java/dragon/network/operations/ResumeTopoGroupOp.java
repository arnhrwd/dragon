package dragon.network.operations;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.ResumeTopoErrorNMsg;
import dragon.network.messages.node.ResumeTopoNMsg;
import dragon.network.messages.node.TopoResumedNMsg;

public class ResumeTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -8685556477082460093L;
	private final String topologyId;
	public ResumeTopoGroupOp(String topologyId,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.topologyId = topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new ResumeTopoNMsg(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoResumedNMsg(topologyId);
	}
	
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new ResumeTopoErrorNMsg(topologyId,error);
	}
}
