package dragon.network.operations;

import dragon.DragonRequiresClonableException;
import dragon.network.Node;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.node.JarReadyMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorMessage;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyRunningMessage;
import dragon.topology.DragonTopology;

public class RunTopologyGroupOperation extends GroupOperation {
	private static final long serialVersionUID = -2038551040445600017L;
	private RunTopologyMessage rtm;
	private transient DragonTopology dragonTopology;
	private transient Node node;
	public RunTopologyGroupOperation(RunTopologyMessage orig,Node node) {
		super(arbridged(orig));
		this.dragonTopology=orig.dragonTopology;
		this.rtm=orig;
		this.rtm.dragonTopology=null;
		this.node=node;
	}
	
	private static RunTopologyMessage arbridged(RunTopologyMessage rtm) {
		RunTopologyMessage rtmlocal = new RunTopologyMessage(rtm.topologyName,rtm.conf,null);
		rtmlocal.setMessageId(rtm.getMessageId());
		return rtmlocal;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareJarMessage(rtm.topologyName,node.readJarFile(rtm.topologyName));
	}
	@Override
	protected NodeMessage successNodeMessage() {
		return new JarReadyMessage(rtm.topologyName);
	}
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new PrepareJarErrorMessage(rtm.topologyName,error);
	}
	@Override
	protected ServiceMessage successServiceMessage() {
		return new TopologyRunningMessage(rtm.topologyName);
	}
	@Override
	protected ServiceMessage failServiceMessage(String error) {
		return new RunTopologyErrorMessage(rtm.topologyName,error);
	}
	
	@Override
	public void success(IComms comms) {
		this.rtm.dragonTopology=dragonTopology;
		PrepareTopologyGroupOperation ptgo = new PrepareTopologyGroupOperation(rtm,node);
		for(NodeDescriptor desc : dragonTopology.getReverseEmbedding().keySet()) {
			ptgo.add(desc);
		}
		node.register(ptgo);
		ptgo.initiate(comms);
		try {
			node.prepareTopology(rtm.topologyName, rtm.conf, dragonTopology, false);
			ptgo.receiveSuccess(comms,comms.getMyNodeDescriptor());
		} catch (DragonRequiresClonableException e) {
			ptgo.receiveError(comms, comms.getMyNodeDescriptor(), e.getMessage());
		}
		
	}

}
