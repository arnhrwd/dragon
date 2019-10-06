package dragon.network.messages.service;

public class TopologyHaltedMessage extends ServiceMessage {
	private static final long serialVersionUID = 989132255201705121L;
	public final String topologyId;
	public TopologyHaltedMessage(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_HALTED);
		this.topologyId=topologyId;
	}

}
