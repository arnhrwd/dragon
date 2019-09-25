package dragon.network;

import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.node.TopologyReadyMessage;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.topology.DragonTopology;

public class PrepareTopologyGroupOperation extends GroupOperation {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7223966055440319387L;
	private RunTopologyMessage rtm;
	private transient Node node;
	public PrepareTopologyGroupOperation(RunTopologyMessage orig,Node node) {
		super(orig);
		this.rtm=orig;
		this.node=node;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareTopologyMessage(rtm.topologyName,
				node.getLocalClusters().get(rtm.topologyName).getConf(),
				node.getLocalClusters().get(rtm.topologyName).getTopology());
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyReadyMessage(rtm.topologyName);
	}
	
	@Override
	public void success(IComms comms) {
		StartTopologyGroupOperation stgo = new StartTopologyGroupOperation(rtm,node);
		DragonTopology dragonTopology = node.getLocalClusters().get(rtm.topologyName).getTopology();
		for(NodeDescriptor desc : dragonTopology.getReverseEmbedding().keySet()) {
			stgo.add(desc);
		}
		node.register(stgo);
		stgo.initiate(comms);
		
		node.startTopology(rtm.topologyName);
		stgo.receiveSuccess(comms, comms.getMyNodeDescriptor());
	}

}
