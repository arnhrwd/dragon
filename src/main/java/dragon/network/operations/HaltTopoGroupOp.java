package dragon.network.operations;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.HaltTopoErrorNMsg;
import dragon.network.messages.node.HaltTopoNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.TopoHaltedNMsg;

/**
 * @author aaron
 *
 */
public class HaltTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 7324344914355135103L;
	
	/**
	 * 
	 */
	private final String topologyId;
	
	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 */
	public HaltTopoGroupOp(String topologyId,IOpSuccess success, IOpFailure failure) {
		super(success,failure);
		this.topologyId = topologyId;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new HaltTopoNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoHaltedNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new HaltTopoErrorNMsg(topologyId,error);
	}

}
