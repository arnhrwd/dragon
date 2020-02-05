package dragon.network.messages.service.getmetrics;

import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class MetricsSMsg extends ServiceMessage {
	private static final long serialVersionUID = -3032101879295800403L;
	
	/**
	 * 
	 */
	public ComponentMetricMap samples;
	
	/**
	 * @param samples
	 */
	public MetricsSMsg(ComponentMetricMap samples) {
		super(ServiceMessage.ServiceMessageType.METRICS);
		this.samples=samples;
	}

}
