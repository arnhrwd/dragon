package dragon.network.messages.service;

public class MetricsErrorMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 779753438805275630L;

	public MetricsErrorMessage() {
		super(ServiceMessage.ServiceMessageType.METRICS_ERROR);
	}
	
}
