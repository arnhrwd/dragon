package dragon.network.messages.node.deallocpart;

import dragon.network.messages.node.NodeMessage;

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

}
