package dragon.network.messages.service;

/**
 * @author aaron
 *
 */
public class AllocPartSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4383113891826020623L;
	
	/**
	 * The name of the partition to allocate
	 */
	public final String partitionId;
	
	/**
	 * Number of daemons (JVMs)
	 */
	public final Integer daemons;
	
	/**
	 * The allocation strategy types
	 * <li>{@link #EACH}</li>
	 * <li>{@link #UNIFORM}</li>
	 * <li>{@link #BALANCED}</li>
	 * @author aaron
	 *
	 */
	public static enum Strategy {
		/**
		 * Allocate the same number on each machine.
		 */
		EACH,
		
		/**
		 * Allocate the number uniformly spread over
		 * the machines.
		 */
		UNIFORM,
		
		/**
		 * Allocate the number spread over the machines
		 * so as to balance the machine load as much as
		 * possible.
		 */
		BALANCED
	}
	
	/**
	 * The allocation strategy to use when allocating a partition.
	 */
	public final Strategy strategy;
	
	/**
	 * @param partitionId
	 * @param daemons
	 * @param strategy
	 */
	public AllocPartSMsg(String partitionId,Integer daemons,Strategy strategy) {
		super(ServiceMessage.ServiceMessageType.ALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.strategy=strategy;
	}

}
