package dragon.network.messages.service;

public class TopologyResumedMessage extends ServiceMessage {
	private static final long serialVersionUID = -8899838211586131880L;
	public final String topologyId;
	public TopologyResumedMessage(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_RESUMED);
		this.topologyId=topologyId;
	}

}
