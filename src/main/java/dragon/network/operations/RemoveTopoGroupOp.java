package dragon.network.operations;


import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.removetopo.RemoveTopoErrorNMsg;
import dragon.network.messages.node.removetopo.RemoveTopoNMsg;
import dragon.network.messages.node.removetopo.TopoRemovedNMsg;


/**
 * @author aaron
 *
 */
public class RemoveTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 7871246034104368201L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * 
	 */
	public final boolean purge;
	
	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 */
	public RemoveTopoGroupOp(String topologyId,boolean purge,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.topologyId=topologyId;
		this.purge=purge;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new RemoveTopoNMsg(topologyId,purge);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoRemovedNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override 
	protected NodeMessage errorNodeMessage(String error) {
		return new RemoveTopoErrorNMsg(topologyId,error);
	}
	
}
