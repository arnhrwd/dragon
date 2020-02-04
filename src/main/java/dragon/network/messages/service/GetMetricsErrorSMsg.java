package dragon.network.messages.service;

/**
 * @author aaron
 *
 */
public class GetMetricsErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 779753438805275630L;
	
	/**
	 * @param error
	 */
	public GetMetricsErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.GET_METRICS_ERROR,error);
	}
	
}
