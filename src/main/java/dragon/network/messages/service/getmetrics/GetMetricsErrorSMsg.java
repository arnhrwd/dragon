package dragon.network.messages.service.getmetrics;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

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
