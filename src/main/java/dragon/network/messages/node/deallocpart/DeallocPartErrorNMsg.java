package dragon.network.messages.node.deallocpart;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * 
 * @author aaron
 *
 */
public class DeallocPartErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * The partition in error.
	 */
	public final String partitionId;
	
	/**
	 * The number of daemons actually deallocated.
	 */
	public final Integer number;
	
	/**
	 * @param partitionId
	 * @param number
	 * @param error
	 */
	public DeallocPartErrorNMsg(String partitionId,Integer number,String error) {
		super(NodeMessage.NodeMessageType.DEALLOCATE_PARTITION_ERROR,error);
		this.partitionId=partitionId;
		this.number=number;
	}
}
