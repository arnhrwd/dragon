package dragon.network.messages.service.halttopo;

import java.util.concurrent.TimeUnit;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.DragonInvalidContext;
import dragon.network.operations.HaltTopoGroupOp;
import dragon.network.operations.Ops;

/**
 * Halt the topology. A halted topology can be resumed.
 * 
 * @author aaron
 *
 */
public class HaltTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = 4205778402629198684L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public HaltTopoSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.HALT_TOPOLOGY);
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
			client(new HaltTopoErrorSMsg(topologyId, "topology does not exist"));
		} else {
			try {
				Ops.inst().newHaltTopoGroupOp(topologyId, (op)->{
					progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
				},(op) -> {
					client(new TopoHaltedSMsg(topologyId));
				}, (op, error) -> {
					client(new HaltTopoErrorSMsg(topologyId, error));
				}).onRunning((op) -> {
					try {
						node.haltTopology(topologyId);
						((HaltTopoGroupOp) op).receiveSuccess(comms.getMyNodeDesc());
					} catch (DragonTopologyException | DragonInvalidStateException e) {
						((HaltTopoGroupOp) op).receiveError(comms.getMyNodeDesc(),e.getMessage());
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
