package dragon.network.messages.node;

import dragon.network.messages.IErrorMessage;

public class AllocPartErrorNMsg extends NodeMessage implements IErrorMessage {
	private static final long serialVersionUID = 6657104194854547858L;
	public final String partitionId;
	public final Integer daemons;
	public final String error;
	public AllocPartErrorNMsg(String partitionId,Integer daemons,String error) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION_ERROR);
		this.partitionId=partitionId;
		this.daemons=daemons;
		this.error=error;
	}
	@Override
	public String getError() {
		return error;
	}

}
