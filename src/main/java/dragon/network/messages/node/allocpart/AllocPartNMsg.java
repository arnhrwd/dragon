package dragon.network.messages.node.allocpart;

import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;
import dragon.network.operations.AllocPartGroupOp;

/**
 * Request the node to allocate a partition.
 * @author aaron
 *
 */
public class AllocPartNMsg extends NodeMessage {
	private static final long serialVersionUID = -5781079273919827198L;
	
	/**
	 * The partition id to allocate.
	 */
	public final String partitionId;
	
	/**
	 * The number of daemons to allocate. 
	 */
	public final Integer number;
	
	/**
	 * @param partitionId the partition id to allocate.
	 * @param number the number of daemons to allocate.
	 */
	public AllocPartNMsg(String partitionId,Integer number) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.number=number;
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		AllocPartGroupOp apgo = (AllocPartGroupOp) getGroupOp();
		int a=node.allocatePartition(partitionId, number);
		apgo.partitionId=partitionId;
		apgo.number=a;
		if(a==number) {
			apgo.sendSuccess();
		} else {
			apgo.sendError("failed to allocate partitions on ["+getSender()+"]");
		}
	}

}
