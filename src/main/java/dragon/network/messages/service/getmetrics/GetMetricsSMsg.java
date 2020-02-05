package dragon.network.messages.service.getmetrics;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class GetMetricsSMsg extends ServiceMessage {
	private static final long serialVersionUID = -5047795658690211908L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public GetMetricsSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.GET_METRICS);
		this.topologyId=topologyId;
	}

}
