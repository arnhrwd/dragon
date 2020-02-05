package dragon.network.operations;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.starttopo.StartTopoErrorNMsg;
import dragon.network.messages.node.starttopo.StartTopoNMsg;
import dragon.network.messages.node.starttopo.TopoStartedNMsg;


/**
 * @author aaron
 *
 */
public class StartTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 2635749611866470029L;
	
	/**
	 * 
	 */
	private final String topologyId;
	
	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 */
	public StartTopoGroupOp(String topologyId,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.topologyId=topologyId;
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new StartTopoNMsg(topologyId);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoStartedNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new StartTopoErrorNMsg(topologyId,error);
	}

}