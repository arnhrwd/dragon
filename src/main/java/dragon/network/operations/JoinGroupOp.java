package dragon.network.operations;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.AcceptingJoinNMsg;
import dragon.network.messages.node.JoinRequestNMsg;
import dragon.network.messages.node.NodeMessage;

public class JoinGroupOp extends GroupOp {
	private static final long serialVersionUID = 1229107693638417484L;
	public transient NodeDescriptor next;
	public transient NodeContext context;
	public JoinGroupOp(IOpSuccess success, IOpFailure failure) {
		super(success, failure);
	}
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new JoinRequestNMsg();
	}
	@Override
	protected NodeMessage successNodeMessage() {
		return new AcceptingJoinNMsg(next, context);
	}
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		// TODO Auto-generated method stub
		return null;
	}
}
