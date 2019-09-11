package dragon.network.messages.service;

import dragon.metrics.ComponentMetricMap;

public class MetricsMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3032101879295800403L;
	
	public ComponentMetricMap samples;
	
	public MetricsMessage(ComponentMetricMap samples) {
		super(ServiceMessage.ServiceMessageType.METRICS);
		this.samples=samples;
	}

}
