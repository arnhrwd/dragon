package dragon.network.messages.service.termtopo;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.DragonInvalidStateException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.DragonInvalidContext;
import dragon.network.operations.Ops;
import dragon.network.operations.RemoveTopoGroupOp;
import dragon.network.operations.TermTopoGroupOp;
import dragon.topology.DragonTopology;

/**
 * Terminate the topology, which consists of: 
 * <ol>
 * <li> terminate the topology on all daemons</li>
 * <li>remove the topology from the router on all daemons</li>
 * </ol>
 * 
 * @author aaron
 *
 */
public class TermTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = -7620075913134267391L;
	private final static Logger log = LogManager.getLogger(TermTopoSMsg.class);


	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 */
	public boolean purge;

	/**
	 * @param topologyId
	 */
	public TermTopoSMsg(String topologyId,boolean purge) {
		super(ServiceMessage.ServiceMessageType.TERMINATE_TOPOLOGY);
		this.topologyId = topologyId;
		this.purge=purge;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		final IComms comms = node.getComms();
		if (!node.getLocalClusters().containsKey(topologyId)) {
			client(new TermTopoErrorSMsg(topologyId, "topology does not exist"));
		} else {
			final DragonTopology topology = node.getLocalClusters().get(topologyId).getTopology();
			if(purge) {
				try {
					Ops.inst().newRemoveTopoGroupOp(this, topology, (op2)->{
						progress("waiting for up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
					},(op2) -> {
						client(new TopoTermdSMsg(topologyId));
					}, (op2, error) -> {
						client(new TermTopoErrorSMsg(topologyId, error));
					}).onRunning((op2) -> {
						try {
							node.removeTopo(topologyId,purge);
							((RemoveTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
						} catch (DragonTopologyException e) {
							((RemoveTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(),e.getMessage());
						}
					}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op2)->{
						op2.fail("timed out removing topology from memory, possibly some machines are overloaded");
					});
				} catch (DragonInvalidContext e) {
					log.error("should not have an invalid context when purging: "+e.getMessage());
					e.printStackTrace();
					client(new TermTopoErrorSMsg(topologyId, e.getMessage()));
				}
			} else {
				try {
					Ops.inst().newTermTopoGroupOp(topologyId,(op)->{
						progress("stopping spouts and waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds for bolts to finish...");
					},(op) -> {
						try {
							Ops.inst().newRemoveTopoGroupOp(this, topology, (op2)->{
								progress("waiting for up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
							},(op2) -> {
								client(new TopoTermdSMsg(topologyId));
							}, (op2, error) -> {
								client(new TermTopoErrorSMsg(topologyId, error));
							}).onRunning((op2) -> {
								try {
									node.removeTopo(topologyId,purge);
									((RemoveTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
								} catch (DragonTopologyException e) {
									((RemoveTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(),e.getMessage());
								}
								progress("removing topology from memory");
							}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op2)->{
								op2.fail("timed out removing topology from memory");
							});
						} catch (DragonInvalidContext e) {
							client(new TermTopoErrorSMsg(topologyId, e.getMessage()));
						}

					}, (op, error) -> {
						client(new TermTopoErrorSMsg(topologyId, error));
					}).onRunning((op) -> {
						try {
							// starts a thread to stop the topology
							node.terminateTopology(topologyId, (TermTopoGroupOp) op);
						} catch (DragonTopologyException | DragonInvalidStateException e) {
							((TermTopoGroupOp) op).fail(e.getMessage());
						}
					}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
						progress("topology is not finishing, possibly mis-behaving user code... will try purging...");
						op.cancel();
						purge=true;
						try {
							Ops.inst().newRemoveTopoGroupOp(this, topology, (op2)->{
								progress("waiting for up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
							},(op2) -> {
								client(new TopoTermdSMsg(topologyId));
							}, (op2, error) -> {
								client(new TermTopoErrorSMsg(topologyId, error));
							}).onRunning((op2) -> {
								try {
									node.removeTopo(topologyId,false);
									((RemoveTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
								} catch (DragonTopologyException e) {
									((RemoveTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(),e.getMessage());
								}
								progress("removing topology from memory");
							}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op2)->{
								op2.fail("timed out removing topology from memory");
							});
						} catch (DragonInvalidContext e) {
							log.error("should not have an invalid context when purging: "+e.getMessage());
							e.printStackTrace();
							client(new TermTopoErrorSMsg(topologyId, e.getMessage()));
						}
					});
				} catch (DragonInvalidContext e) {
					client(new TermTopoErrorSMsg(topologyId, e.getMessage()));
				}
			}
		}
	}

}
