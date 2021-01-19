package dragon.network.messages.node.deallocpart;

import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;
import dragon.network.operations.DeallocPartGroupOp;

/**
 * 
 * @author aaron
 *
 */
public class DeallocPartNMsg extends NodeMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * The partition id to deallocate.
	 */
	public final String partitionId;
	
	/**
	 * The number of daemons to deallocate. 
	 */
	public final Integer number;
	
	/**
	 * @param partitionId the partition id to deallocate.
	 * @param number the number of daemons to deallocate.
	 */
	public DeallocPartNMsg(String partitionId,Integer number) {
		super(NodeMessage.NodeMessageType.DEALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.number=number;
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		DeallocPartGroupOp apgo = (DeallocPartGroupOp) getGroupOp();
		int a=node.deallocatePartition(partitionId, number);
		apgo.partitionId=partitionId;
		apgo.number=a;
		if(a==number) {
			apgo.sendSuccess();
		} else {
			apgo.sendError("failed to deallocate partitions on ["+getSender()+"]");
		}
	}

}
