package dragon.network.messages.service;

public class GetNodeContextMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -640901729958656459L;

	public GetNodeContextMessage() {
		super(ServiceMessage.ServiceMessageType.GET_NODE_CONTEXT);
	}

}
