package dragon.network.messages.service;

public class GetMetricsMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5047795658690211908L;
	public String topologyId;
	public GetMetricsMessage(String topologyId) {
		super(ServiceMessage.ServiceMessageType.GET_METRICS);
	}

}
