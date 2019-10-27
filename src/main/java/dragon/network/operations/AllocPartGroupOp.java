package dragon.network.operations;

import java.util.HashMap;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.AllocPartErrorNMsg;
import dragon.network.messages.node.AllocPartNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PartAllocedNMsg;

public class AllocPartGroupOp extends GroupOp {
	private static final long serialVersionUID = -5039879170683508607L;
	public transient final String partitionId;
	public transient final HashMap<NodeDescriptor,Integer> allocation;
	public transient int daemons;
	public AllocPartGroupOp(String partitionId,HashMap<NodeDescriptor,Integer> allocation,IOpSuccess success, IOpFailure failure) {
		super(success, failure);
		this.partitionId=partitionId;
		this.allocation=allocation;
	}

	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new AllocPartNMsg(partitionId,allocation.get(desc));
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new PartAllocedNMsg(partitionId,daemons);
	}

	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new AllocPartErrorNMsg(partitionId,daemons,error);
	}

}
