package dragon.network;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StartTopologyMessage;
import dragon.network.messages.node.TopologyStartedMessage;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyRunningMessage;

public class StartTopologyGroupOperation extends GroupOperation {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2635749611866470029L;
	private RunTopologyMessage rtm;
	private transient Node node;
	public StartTopologyGroupOperation(RunTopologyMessage orig,Node node) {
		super(orig);
		this.rtm=orig;
		this.node=node;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new StartTopologyMessage(rtm.topologyName);
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyStartedMessage(rtm.topologyName);
	}

	@Override
	protected ServiceMessage successServiceMessage() {
		return new TopologyRunningMessage(rtm.topologyName);
	}

}
