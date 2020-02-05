package dragon.network.operations;

import java.util.HashMap;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.deallocpart.DeallocPartErrorNMsg;
import dragon.network.messages.node.deallocpart.DeallocPartNMsg;
import dragon.network.messages.node.deallocpart.PartDeallocedNMsg;

/**
 * 
 * @author aaron
 *
 */
public class DeallocPartGroupOp extends GroupOp {
	private static final long serialVersionUID = 3416126439551090828L;

	/**
	 * 
	 */
	public transient String partitionId;
	
	/**
	 * 
	 */
	public transient final HashMap<NodeDescriptor,Integer> allocation;
	
	/**
	 * 
	 */
	public transient int number;
	
	/**
	 * @param partitionId
	 * @param allocation
	 * @param success
	 * @param failure
	 */
	public DeallocPartGroupOp(String partitionId,HashMap<NodeDescriptor,Integer> allocation,IOpSuccess success, IOpFailure failure) {
		super(success, failure);
		this.partitionId=partitionId;
		this.allocation=allocation;
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new DeallocPartNMsg(partitionId,allocation.get(desc));
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new PartDeallocedNMsg(partitionId,number);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new DeallocPartErrorNMsg(partitionId,number,error);
	}
}
