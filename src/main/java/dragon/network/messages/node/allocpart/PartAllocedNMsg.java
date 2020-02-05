package dragon.network.messages.node.allocpart;

import dragon.network.messages.node.NodeMessage;

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
	public final Integer number;
	
	/**
	 * @param partitionId
	 * @param number
	 */
	public PartAllocedNMsg(String partitionId,Integer number) {
		super(NodeMessage.NodeMessageType.PARTITION_ALLOCATED);
		this.partitionId=partitionId;
		this.number=number;
	}

}
