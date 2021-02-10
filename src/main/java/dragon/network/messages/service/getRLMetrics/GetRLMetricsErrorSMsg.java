package dragon.network.messages.service.getRLMetrics;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class GetRLMetricsErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = 779753438805275630L;
	
	/**
	 * @param error
	 */
	public GetRLMetricsErrorSMsg(String error) {
		super(ServiceMessage.ServiceMessageType.GET_RL_METRICS_ERROR,error);
	}
	
}
