package dragon.network.operations;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.node.TopologyStartedMessage;
import dragon.network.messages.service.RunTopologyMessage;


public class StartTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 2635749611866470029L;
	private final RunTopologyMessage rtm;
	
	public StartTopoGroupOp(RunTopologyMessage orig,IOpSuccess success,IOpFailure failure) {
		super(orig,success,failure);
		this.rtm=orig;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new StartTopologyMessage(rtm.topologyName);
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyStartedMessage(rtm.topologyName);
	}

}
