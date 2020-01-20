package dragon.network.messages.service;

public class TopoTermdSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4461190987886019247L;
	public final String topologyId;
	public TopoTermdSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_TERMINATED);
		this.topologyId=topologyId;
	}

}
