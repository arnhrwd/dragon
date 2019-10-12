package dragon.network.operations;

import dragon.network.messages.Message;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.RouterTerminatedMessage;
import dragon.network.messages.node.TerminateRouterErrorMessage;
import dragon.network.messages.node.TerminateRouterMessage;


public class TermRouterGroupOp extends GroupOp {
	private static final long serialVersionUID = 7871246034104368201L;
	public final String topologyId;
	
	public TermRouterGroupOp(Message orig,String topologyId,IOpSuccess success,IOpFailure failure) {
		super(orig,success,failure);
		this.topologyId=topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new TerminateRouterMessage(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new RouterTerminatedMessage(topologyId);
	}
	
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new TerminateRouterErrorMessage(topologyId,error);
	}
	
}
