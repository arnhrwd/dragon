package dragon.network.messages.service;

public class TopologyTerminatedMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4461190987886019247L;
	String topologyId;
	public TopologyTerminatedMessage(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_TERMINATED);
		this.topologyId=topologyId;
	}

}
