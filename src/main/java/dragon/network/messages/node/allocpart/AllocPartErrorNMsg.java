package dragon.network.messages.node.allocpart;

import dragon.network.messages.node.NodeErrorMessage;
import dragon.network.messages.node.NodeMessage;

/**
 * Error attempting to allocate a partition.
 * @author aaron
 *
 */
public class AllocPartErrorNMsg extends NodeErrorMessage {
	private static final long serialVersionUID = 6657104194854547858L;
	
	/**
	 * The partition in error.
	 */
	public final String partitionId;
	
	/**
	 * The number of daemons actually allocated.
	 */
	public final Integer number;
	
	/**
	 * @param partitionId
	 * @param number
	 * @param error
	 */
	public AllocPartErrorNMsg(String partitionId,Integer number,String error) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION_ERROR,error);
		this.partitionId=partitionId;
		this.number=number;
	}
	
}
