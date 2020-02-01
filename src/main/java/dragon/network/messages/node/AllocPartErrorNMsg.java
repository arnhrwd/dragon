package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

/**
 * @author aaron
 *
 */
public class AllocPartErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = 6657104194854547858L;
	
	/**
	 * 
	 */
	public final String partitionId;
	
	/**
	 * 
	 */
	public final Integer daemons;
	
	/**
	 * 
	 */
	public final String error;
	
	/**
	 * @param partitionId
	 * @param daemons
	 * @param error
	 */
	public AllocPartErrorNMsg(String partitionId,Integer daemons,String error) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION_ERROR);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.error=error;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.messages.IErrorMessage#getError()
	 */
	@Override
	public String getError() {
		return error;
	}

}
