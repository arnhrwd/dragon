package dragon.network.messages.node;

/**
 * @author aaron
 *
 */
public class PartAllocedNMsg extends NodeMessage {
	private static final long serialVersionUID = -1749036131669394059L;
	
	/**
	 * 
	 */
	public final String partitionId;
	
	/**
	 * 
	 */
	public final Integer daemons;
	
	/**
	 * @param partitionId
	 * @param daemons
	 */
	public PartAllocedNMsg(String partitionId,Integer daemons) {
		super(NodeMessage.NodeMessageType.PARTITION_ALLOCATED);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}

}
