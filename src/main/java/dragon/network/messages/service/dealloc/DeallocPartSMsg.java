package dragon.network.messages.service.dealloc;

import dragon.network.messages.service.ServiceMessage;

/**
 * Deallocate a partition.
 * @author aaron
 *
 */
public class DeallocPartSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1L;

	/**
	 * The name of the partition to deallocate
	 */
	public final String partitionId;
	
	/**
	 * Number of daemons (JVMs) to remove
	 */
	public final Integer daemons;
	
	/**
	 * The deallocate strategy types
	 * <li>{@link #EACH}</li>
	 * <li>{@link #UNIFORM}</li>
	 * <li>{@link #BALANCED}</li>
	 * @author aaron
	 *
	 */
	public static enum Strategy {
		/**
		 * Deallocate the same number on each machine.
		 */
		EACH,
		
		/**
		 * Deallocate the number uniformly spread over
		 * the machines.
		 */
		UNIFORM,
		
		/**
		 * Deallocate the number spread over the machines
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
	 * 
	 * @param partitionId
	 * @param daemons
	 * @param strategy
	 */
	public DeallocPartSMsg(String partitionId,Integer daemons,Strategy strategy) {
		super(ServiceMessage.ServiceMessageType.DEALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.strategy=strategy;
	}

}
