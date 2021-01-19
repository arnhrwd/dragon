package dragon.network.messages.service.getmetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.metrics.ComponentMetricMap;
import dragon.network.Node;
import dragon.network.messages.service.ServiceMessage;

/**
 * Get metrics for this daemon only.
 * 
 * @author aaron
 *
 */
public class GetMetricsSMsg extends ServiceMessage {
	private static final long serialVersionUID = -5047795658690211908L;
	private final static Logger log = LogManager.getLogger(GetMetricsSMsg.class);
	
	
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
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		if ((Boolean) node.getConf().getDragonMetricsEnabled()) {
			ComponentMetricMap cm = node.getMetrics(topologyId);
			if (cm != null) {
				client(new MetricsSMsg(cm));
			} else {
				client(new GetMetricsErrorSMsg("unknown topology or there are no samples available yet"));
			}
		} else {
			log.warn("metrics are not enabled");
			client(new GetMetricsErrorSMsg("metrics are not enabled in dragon.yaml for this node"));
		}
	}

}
