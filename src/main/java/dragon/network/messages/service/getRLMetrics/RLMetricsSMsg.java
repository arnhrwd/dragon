package dragon.network.messages.service.getRLMetrics;

import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class RLMetricsSMsg extends ServiceMessage {
	private static final long serialVersionUID = -3032101879295800403L;
	
	/**
	 * 
	 */
	public ComponentMetricMap samples;
	
	/**
	 * @param samples
	 */
	public RLMetricsSMsg(ComponentMetricMap samples) {
		super(ServiceMessage.ServiceMessageType.RL_METRICS);
		this.samples=samples;
	}

}
