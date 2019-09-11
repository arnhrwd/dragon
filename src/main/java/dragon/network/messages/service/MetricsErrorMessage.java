package dragon.network.messages.service;

public class MetricsErrorMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 779753438805275630L;
	public String error;
	public MetricsErrorMessage(String error) {
		super(ServiceMessage.ServiceMessageType.METRICS_ERROR);
		this.error=error;
	}
	
}
