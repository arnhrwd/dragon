package dragon.network.messages.service.getRLMetrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.metrics.Sample;
import dragon.metrics.TopologyMetricMap;
import dragon.network.Node;
import dragon.network.messages.service.ServiceMessage;

/**
 * Get metrics for this daemon only.
 * 
 * @author aaron
 *
 */
public class GetRLMetricsSMsg extends ServiceMessage {
	private static final long serialVersionUID = -5047795658690211908L;
	private final static Logger log = LogManager.getLogger(GetRLMetricsSMsg.class);
	private final TopologyMetricMap samples;

	/**
	 * 
	 */
	public String topologyId;

	/**
	 * @param topologyId
	 */
	public GetRLMetricsSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.GET_RL_METRICS);
		this.topologyId = topologyId;
		samples = new TopologyMetricMap(1);
	}

	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		LocalCluster localCluster = node.getLocalClusters().get(topologyId);
		ComponentMetricMap cm = null;
		if (localCluster != null) {
			for (String componentId : localCluster.getSpouts().keySet()) {
				for (Integer taskId : localCluster.getSpouts().get(componentId).keySet()) {
					Sample sample = new Sample(localCluster.getSpouts().get(componentId).get(taskId));

					samples.put(topologyId, componentId, taskId, sample);
				}
			}
			for (String componentId : localCluster.getBolts().keySet()) {
				for (Integer taskId : localCluster.getBolts().get(componentId).keySet()) {
					Sample sample = new Sample(localCluster.getBolts().get(componentId).get(taskId));

					samples.put(topologyId, componentId, taskId, sample);
				}
			}

			cm = samples.get(topologyId);
		}
		if (cm != null) {
			client(new RLMetricsSMsg(cm));
		} else {
			client(new GetRLMetricsErrorSMsg("unknown topology or there are no samples available yet"));
		}
	}

}
