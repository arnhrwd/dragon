package dragon.network.operations;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.join.AcceptingJoinNMsg;
import dragon.network.messages.node.join.JoinRequestNMsg;

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
	public JoinGroupOp(IComms comms,IOpSuccess success, IOpFailure failure) {
		super(comms,success, failure);
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
