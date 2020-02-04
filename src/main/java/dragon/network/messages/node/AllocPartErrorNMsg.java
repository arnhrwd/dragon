package dragon.network.messages.node;

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
	public final Integer daemons;
	
	/**
	 * @param partitionId
	 * @param daemons
	 * @param error
	 */
	public AllocPartErrorNMsg(String partitionId,Integer daemons,String error) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION_ERROR,error);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}
	
}
