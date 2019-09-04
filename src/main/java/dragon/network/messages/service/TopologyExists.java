package dragon.network.messages.service;

public class TopologyExists extends ServiceMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 950825340997670248L;
	public String topologyName;
	public TopologyExists(String name) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_EXISTS);
		topologyName=name;
	}
	
}
