package dragon.network.messages.service.dealloc;

import dragon.network.messages.service.ServiceMessage;

/**
 * 
 * @author aaron
 *
 */
public class PartDeallocedSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public final String partitionId;
	
	/**
	 * 
	 */
	public final Integer number;
	
	/**
	 * 
	 * @param partitionId
	 * @param number
	 */
	public PartDeallocedSMsg(String partitionId,Integer number) {
		super(ServiceMessage.ServiceMessageType.PARTITION_DEALLOCATED);
		this.partitionId=partitionId;
		this.number=number;
	}

}
