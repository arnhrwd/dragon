package dragon.network.messages.service;

public class AllocPartSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4383113891826020623L;
	public final String partitionId;
	public final Integer daemons;
	public static enum Strategy {
		PER_PRIMARY,
		UNIFORMLY,
		LEAST_LOADED
	}
	public final Strategy strategy;
	public AllocPartSMsg(String partitionId,Integer daemons,Strategy strategy) {
		super(ServiceMessage.ServiceMessageType.ALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.strategy=strategy;
	}

}
