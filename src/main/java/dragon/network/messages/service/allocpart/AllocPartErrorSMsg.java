package dragon.network.messages.service.allocpart;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class AllocPartErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = -753259506397468279L;
	
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
	 * @param error
	 */
	public AllocPartErrorSMsg(String partitionId,Integer daemons,String error) {
		super(ServiceMessage.ServiceMessageType.ALLOCATE_PARTITION_ERROR,error);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}

}
