package dragon.network.operations;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.ResumeTopoErrorNMsg;
import dragon.network.messages.node.ResumeTopoNMsg;
import dragon.network.messages.node.TopoResumedNMsg;

/**
 * @author aaron
 *
 */
public class ResumeTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -8685556477082460093L;
	
	/**
	 * 
	 */
	private final String topologyId;
	
	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 */
	public ResumeTopoGroupOp(String topologyId,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.topologyId = topologyId;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new ResumeTopoNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoResumedNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new ResumeTopoErrorNMsg(topologyId,error);
	}
}
