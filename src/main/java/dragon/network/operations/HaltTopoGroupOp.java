package dragon.network.operations;

import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.halttopo.HaltTopoErrorNMsg;
import dragon.network.messages.node.halttopo.HaltTopoNMsg;
import dragon.network.messages.node.halttopo.TopoHaltedNMsg;

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
	public HaltTopoGroupOp(IComms comms,String topologyId,IOpStart start,IOpSuccess success, IOpFailure failure) {
		super(comms,start,success,failure);
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
