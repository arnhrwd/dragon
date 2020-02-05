package dragon.network.messages.service.runtopo;

import dragon.Config;
import dragon.network.messages.service.ServiceMessage;
import dragon.topology.DragonTopology;

/**
 * @author aaron
 *
 */
public class RunTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1511393375978089832L;

	/**
	 * 
	 */
	public DragonTopology dragonTopology;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 */
	public Config conf;
	
	/**
	 * @param topologyName
	 * @param conf
	 * @param dragonTopology
	 */
	public RunTopoSMsg(String topologyName, Config conf, DragonTopology dragonTopology) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY);
		this.dragonTopology = dragonTopology;
		this.conf=conf;
		this.topologyId=topologyName;
	}
	
}
