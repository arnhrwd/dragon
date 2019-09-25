package dragon.network;


import dragon.LocalCluster;
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

	/**
	 * 
	 */
	private static final long serialVersionUID = -2038551040445600017L;
	private RunTopologyMessage rtm;
	private transient Node node;
	public RunTopologyGroupOperation(RunTopologyMessage orig,Node node) {
		super(orig);
		this.rtm=orig;
		this.node=node;
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
		PrepareTopologyGroupOperation ptgo = new PrepareTopologyGroupOperation(rtm,node);
		DragonTopology dragonTopology = node.getLocalClusters().get(rtm.topologyName).getTopology();
		for(NodeDescriptor desc : dragonTopology.getReverseEmbedding().keySet()) {
			ptgo.add(desc);
		}
		node.register(ptgo);
		ptgo.initiate(comms);
		LocalCluster cluster=new LocalCluster(node);
		cluster.submitTopology(rtm.topologyName, rtm.conf, rtm.dragonTopology, false);
		node.getRouter().submitTopology(rtm.topologyName,rtm.dragonTopology);
		node.getLocalClusters().put(rtm.topologyName, cluster);
		ptgo.receiveSuccess(comms,comms.getMyNodeDescriptor());
	}

}
