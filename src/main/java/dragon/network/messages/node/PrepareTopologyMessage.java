package dragon.network.messages.node;

import dragon.Config;
import dragon.topology.DragonTopology;

public class PrepareTopologyMessage extends NodeMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2867515610457893626L;
	public DragonTopology topology;
	public String topologyName;
	public Config conf;

	public PrepareTopologyMessage(String topologyName, Config conf, DragonTopology dragonTopology) {
		super(NodeMessage.NodeMessageType.PREPARE_TOPOLOGY);
		this.topologyName=topologyName;
		this.topology=dragonTopology;
		this.conf=conf;
		
	}

}
