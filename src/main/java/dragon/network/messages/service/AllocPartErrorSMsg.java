package dragon.network.messages.service;

import dragon.network.messages.IErrorMessage;

public class AllocPartErrorSMsg extends ServiceMessage  implements IErrorMessage {
	private static final long serialVersionUID = -753259506397468279L;
	public final String partitionId;
	public final Integer daemons;
	public final String error;
	public AllocPartErrorSMsg(String partitionId,Integer daemons,String error) {
		super(ServiceMessage.ServiceMessageType.ALLOCATE_PARTITION_ERROR);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
