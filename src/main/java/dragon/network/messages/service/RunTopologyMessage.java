package dragon.network.messages.service;

import dragon.Config;
import dragon.topology.DragonTopology;

public class RunTopologyMessage extends ServiceMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1511393375978089832L;

	public DragonTopology dragonTopology;
	public String topologyName;
	public Config conf;
	public byte[] topologyJar;
	
	public RunTopologyMessage(String topologyName, Config conf, DragonTopology dragonTopology, byte[] topologyJar) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY);
		this.dragonTopology = dragonTopology;
		this.conf=conf;
		this.topologyJar=topologyJar;
		this.topologyName=topologyName;
	}
	
}
