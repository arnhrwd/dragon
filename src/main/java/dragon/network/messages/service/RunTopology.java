package dragon.network.messages.service;

import dragon.Config;
import dragon.topology.DragonTopology;

public class RunTopology extends ServiceMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1511393375978089832L;

	public DragonTopology dragonTopology;
	public String topologyName;
	public Config conf;
	
	public RunTopology(String topologyName, Config conf, DragonTopology dragonTopology) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY);
		this.dragonTopology = dragonTopology;
	}
	
}
