package dragon.network.operations;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.AcceptingJoinNMsg;
import dragon.network.messages.node.JoinRequestNMsg;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class JoinGroupOp extends GroupOp {
	private static final long serialVersionUID = 1229107693638417484L;
	
	/**
	 * 
	 */
	public transient NodeDescriptor next;
	
	/**
	 * 
	 */
	public transient NodeContext context;
	
	/**
	 * @param success
	 * @param failure
	 */
	public JoinGroupOp(IOpSuccess success, IOpFailure failure) {
		super(success, failure);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new JoinRequestNMsg();
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new AcceptingJoinNMsg(next, context);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		// TODO Auto-generated method stub
		return null;
	}
}
