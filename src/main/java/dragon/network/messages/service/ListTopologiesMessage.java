package dragon.network.messages.service;

public class ListTopologiesMessage extends ServiceMessage {
	private static final long serialVersionUID = -8279553169106206333L;
	public ListTopologiesMessage() {
		super(ServiceMessage.ServiceMessageType.LIST_TOPOLOGIES);
	}

}
