package dragon.network.messages.service;

public class TopologyListMessage extends ServiceMessage {
	private static final long serialVersionUID = -4010036400846816662L;
	public TopologyListMessage() {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_LIST);		
	}
}
