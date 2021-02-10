package dragon.network.messages.service.execRlAction;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.LocalCluster;
import dragon.network.Node;
import dragon.network.messages.service.ServiceMessage;
import dragon.topology.base.Spout;

/**
 * Get metrics for this daemon only.
 * 
 * @author aaron
 *
 */
public class ExecRlActionSMsg extends ServiceMessage {

	private static final long serialVersionUID = -1887105465739798456L;
	private final static Logger log = LogManager.getLogger(ExecRlActionSMsg.class);
	private long delta;

	/**
	 * 
	 */
	public String topologyId;

	/**
	 * @param topologyId
	 */
	public ExecRlActionSMsg(String topologyId, long delta) {
		super(ServiceMessage.ServiceMessageType.EXEC_RL_ACTION);
		this.topologyId = topologyId;
		this.delta = delta;
	}

	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		LocalCluster localCluster = node.getLocalClusters().get(topologyId);
		String error = "";
		if (localCluster != null) {
			if (localCluster.getSpouts().isEmpty()) {
				error = "node has no spouts";
				client(new ExecRlActionErrorSMsg(error));
			} else {
				for (String componentId : localCluster.getSpouts().keySet()) {
					for (Integer taskId : localCluster.getSpouts().get(componentId).keySet()) {
						Spout spout = localCluster.getSpouts().get(componentId).get(taskId);
						//spout.updateDataEmissionIntervalDelta(delta);
						spout.updateDataEmissionInterval(delta);
					}
				}
				client(new RlActionExecutedSMsg(topologyId));
			}
		} else {
			error = "node has no local clusters";
			client(new ExecRlActionErrorSMsg(error));
		}

	}

}
