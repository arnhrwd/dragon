package dragon.network.messages.service.allocpart;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

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
	public final Integer daemons;
	
	/**
	 * @param partitionId
	 * @param daemons
	 */
	public PartAllocedSMsg(String partitionId,Integer daemons) {
		super(ServiceMessage.ServiceMessageType.PARTITION_ALLOCATED);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}

}
