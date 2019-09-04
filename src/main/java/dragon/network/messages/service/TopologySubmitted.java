package dragon.network.messages.service;

public class TopologySubmitted extends ServiceMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2760522646650784474L;
	public String topologyName;
	
	public TopologySubmitted(String topologyName) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_SUBMITTED);
		this.topologyName = topologyName;
	}

}
