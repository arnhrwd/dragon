package dragon.network.messages.service.allocpart;

import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class PartAllocedSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4734158582012835838L;
	
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
	public PartAllocedSMsg(String partitionId,Integer number) {
		super(ServiceMessage.ServiceMessageType.PARTITION_ALLOCATED);
		this.partitionId=partitionId;
		this.number=number;
	}

}
