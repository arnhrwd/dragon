package dragon.network.operations;

import java.util.HashMap;

import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.allocpart.AllocPartErrorNMsg;
import dragon.network.messages.node.allocpart.AllocPartNMsg;
import dragon.network.messages.node.allocpart.PartAllocedNMsg;

/**
 * @author aaron
 *
 */
public class AllocPartGroupOp extends GroupOp {
	private static final long serialVersionUID = -5039879170683508607L;
	
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
	public AllocPartGroupOp(IComms comms,String partitionId,HashMap<NodeDescriptor,Integer> allocation,IOpStart start,IOpSuccess success, IOpFailure failure) {
		super(comms,start,success, failure);
		this.partitionId=partitionId;
		this.allocation=allocation;
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new AllocPartNMsg(partitionId,allocation.get(desc));
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new PartAllocedNMsg(partitionId,number);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new AllocPartErrorNMsg(partitionId,number,error);
	}

}
