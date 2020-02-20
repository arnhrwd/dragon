package dragon.network.messages.service.dealloc;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

/**
 * 
 * @author aaron
 *
 */
public class DeallocPartErrorSMsg extends ServiceErrorMessage {
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
	 * @param partitionId
	 * @param number
	 * @param error
	 */
	public DeallocPartErrorSMsg(String partitionId,Integer number,String error) {
		super(ServiceMessage.ServiceMessageType.DEALLOCATE_PARTITION_ERROR, error);
		this.partitionId=partitionId;
		this.number=number;
	}

}
