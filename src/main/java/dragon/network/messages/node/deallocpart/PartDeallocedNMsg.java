package dragon.network.messages.node.deallocpart;

import dragon.network.messages.node.NodeMessage;

/**
 * 
 * @author aaron
 *
 */
public class PartDeallocedNMsg extends NodeMessage {
	private static final long serialVersionUID = -8703014240357658735L;

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
	public PartDeallocedNMsg(String partitionId,Integer number) {
		super(NodeMessage.NodeMessageType.PARTITION_DEALLOCATED);
		this.partitionId=partitionId;
		this.number=number;
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		receiveSuccess();
	}

}
