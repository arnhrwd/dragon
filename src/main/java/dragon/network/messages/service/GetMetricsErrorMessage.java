package dragon.network.messages.service;

public class GetMetricsErrorMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 779753438805275630L;
	public String error;
	public GetMetricsErrorMessage(String error) {
		super(ServiceMessage.ServiceMessageType.GET_METRICS_ERROR);
		this.error=error;
	}
	
}
