package dragon.network.messages.node;

public class AllocPartNMsg extends NodeMessage {
	private static final long serialVersionUID = -5781079273919827198L;
	public final String partitionId;
	public final Integer daemons;
	public AllocPartNMsg(String partitionId,Integer daemons) {
		super(NodeMessage.NodeMessageType.ALLOCATE_PARTITION);
		this.partitionId=partitionId;
		this.daemons=daemons;
	}

}
