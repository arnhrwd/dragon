package dragon.network.messages.service;

public class GetNodeContextSMsg extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -640901729958656459L;

	public GetNodeContextSMsg() {
		super(ServiceMessage.ServiceMessageType.GET_NODE_CONTEXT);
	}

}
