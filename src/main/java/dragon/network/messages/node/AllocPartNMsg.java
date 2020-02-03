package dragon.network.messages.node;

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
	public final Integer daemons;
	
	/**
	 * @param partitionId the partition id to allocate.
	 * @param daemons the number of daemons to allocate.
	 */
	public AllocPartNMsg(String partitionId,Integer daemons) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}

}
