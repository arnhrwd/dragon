package dragon.network.operations;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StartTopoNMsg;
import dragon.network.messages.node.TopoStartedNMsg;
import dragon.network.messages.service.RunTopoSMsg;


public class StartTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 2635749611866470029L;
	private final RunTopoSMsg rtm;
	
	public StartTopoGroupOp(RunTopoSMsg orig,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.rtm=orig;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new StartTopoNMsg(rtm.topologyId);
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoStartedNMsg(rtm.topologyId);
	}

}
