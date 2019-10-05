package dragon.network.messages.service;


public class ListTopologiesErrorMessage extends ServiceMessage {
	private static final long serialVersionUID = -1697348718172307885L;
	public final String error;
	public ListTopologiesErrorMessage(String error) {
		super(ServiceMessage.ServiceMessageType.LIST_TOPOLOGIES_ERROR);
		this.error=error;
	}

}
