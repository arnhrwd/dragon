package dragon.network.messages.service.resumetopo;

import java.util.concurrent.TimeUnit;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.halttopo.HaltTopoErrorSMsg;
import dragon.network.operations.DragonInvalidContext;
import dragon.network.operations.Ops;
import dragon.network.operations.ResumeTopoGroupOp;

/**
 * Resume a halted topology.
 * 
 * @author aaron
 *
 */
public class ResumeTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4132366816670626369L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public ResumeTopoSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.RESUME_TOPOLOGY);
		this.topologyId=topologyId;
	}

	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		final IComms comms = node.getComms();
		if (!node.getLocalClusters().containsKey(topologyId)) {
			client(new ResumeTopoErrorSMsg(topologyId, "topology does not exist"));
		} else {
			try {
				Ops.inst().newResumeTopoGroupOp(topologyId,(op)->{
					progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
				},	(op) -> {
					client(new TopoResumedMsg(topologyId));
				}, (op, error) -> {
					client(new ResumeTopoErrorSMsg(topologyId, error));
				}).onRunning((op) -> {
					try {
						node.resumeTopology(topologyId);
						((ResumeTopoGroupOp) op).receiveSuccess(comms.getMyNodeDesc());
					} catch (DragonTopologyException | DragonInvalidStateException e) {
						((ResumeTopoGroupOp) op).receiveError(comms.getMyNodeDesc(),e.getMessage());
					}
				}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
					op.fail("timed out waiting for nodes to respond");
				});
			} catch (DragonInvalidContext e) {
				client(new HaltTopoErrorSMsg(topologyId, e.getMessage()));
			}

		}
	}
}
