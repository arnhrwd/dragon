package dragon.network.operations;


import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.node.TopologyReadyMessage;
import dragon.network.messages.service.RunTopologyMessage;


public class PrepareTopologyGroupOperation extends GroupOperation {
	private static final long serialVersionUID = 7223966055440319387L;
	private RunTopologyMessage rtm;
	
	public PrepareTopologyGroupOperation(RunTopologyMessage orig,IOperationSuccess success,
			IOperationFailure failure) {
		super(orig,success,failure);
		this.rtm=orig;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareTopologyMessage(rtm.topologyName,rtm.conf,rtm.dragonTopology);
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyReadyMessage(rtm.topologyName);
	}

}
