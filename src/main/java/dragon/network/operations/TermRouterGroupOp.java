package dragon.network.operations;

import dragon.network.messages.Message;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopoRemovedNMsg;
import dragon.network.messages.node.RemoveTopoErrorNMsg;
import dragon.network.messages.node.RemoveTopoNMsg;


public class TermRouterGroupOp extends GroupOp {
	private static final long serialVersionUID = 7871246034104368201L;
	public final String topologyId;
	
	public TermRouterGroupOp(Message orig,String topologyId,IOpSuccess success,IOpFailure failure) {
		super(orig,success,failure);
		this.topologyId=topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new RemoveTopoNMsg(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoRemovedNMsg(topologyId);
	}
	
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new RemoveTopoErrorNMsg(topologyId,error);
	}
	
}
