package dragon.network.messages.service;

public class TopologySubmittedMessage extends ServiceMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2760522646650784474L;
	public String topologyName;
	
	public TopologySubmittedMessage(String topologyName) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_SUBMITTED);
		this.topologyName = topologyName;
	}

}
