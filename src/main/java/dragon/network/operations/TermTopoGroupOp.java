package dragon.network.operations;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StopTopoErrorNMsg;
import dragon.network.messages.node.StopTopoNMsg;
import dragon.network.messages.node.TopoStoppedNMsg;


/**
 * @author aaron
 *
 */
public class TermTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -7596391746339394369L;
	
	/**
	 * 
	 */
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(TermTopoGroupOp.class);
	
	/**
	 * 
	 */
	private final String topologyId;
	
	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 */
	public TermTopoGroupOp(String topologyId,IOpSuccess success, IOpFailure failure) {
		super(success,failure);
		this.topologyId=topologyId;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new StopTopoNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoStoppedNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new StopTopoErrorNMsg(topologyId,error);
	}

}
