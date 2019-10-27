package dragon.network.messages.service;

public class PartAllocedSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4734158582012835838L;
	public final String partitionId;
	public final Integer daemons;
	public PartAllocedSMsg(String partitionId,Integer daemons) {
		super(ServiceMessage.ServiceMessageType.PARTITION_ALLOCATED);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}

}
