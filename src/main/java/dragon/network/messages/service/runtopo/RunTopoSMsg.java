package dragon.network.messages.service.runtopo;

import java.util.concurrent.TimeUnit;

import dragon.Config;
import dragon.DragonInvalidStateException;
import dragon.DragonRequiresClonableException;
import dragon.network.DragonTopologyException;
import dragon.network.Node;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.DragonInvalidContext;
import dragon.network.operations.Ops;
import dragon.network.operations.PrepJarGroupOp;
import dragon.network.operations.PrepTopoGroupOp;
import dragon.network.operations.StartTopoGroupOp;
import dragon.topology.DragonTopology;

/**
 * Run the topology. Running a topology involves the sequence of group
 * operations:
 * <ol>
 * <li>send the jar file to all daemons</li>
 * <li>allocate the local cluster on all daemons</li>
 * <li>start the local cluster on all daemons</li>
 * </ol>
 * 
 * @author aaron
 *
 */
public class RunTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1511393375978089832L;

	/**
	 * 
	 */
	public DragonTopology dragonTopology;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 */
	public Config conf;
	
	/**
	 * @param topologyName
	 * @param conf
	 * @param dragonTopology
	 */
	public RunTopoSMsg(String topologyName, Config conf, DragonTopology dragonTopology) {
		super(ServiceMessage.ServiceMessageType.RUN_TOPOLOGY);
		this.dragonTopology = dragonTopology;
		this.conf=conf;
		this.topologyId=topologyName;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node=Node.inst();
		final IComms comms=node.getComms();
		if (node.getLocalClusters().containsKey(topologyId)) {
			client(new RunTopoErrorSMsg(topologyId, "topology exists"));
		} else {
			final DragonTopology topo = dragonTopology;
			byte[] jarfile =  node.readJarFile(topologyId);
			if(jarfile==null){
				client(new RunTopoErrorSMsg(topologyId, "could not read the jar file; please upload jar first"));
				return;
			}
			try {
				Ops.inst().newPrepJarGroupOp(topologyId, jarfile, topo, (op) -> {
					try {
						Ops.inst().newPreTopoGroupOp(this, topo, (op2) -> {
							try {
								Ops.inst().newStartTopologyGroupOp(topologyId, (op3) -> {
									client(new TopoRunningSMsg(topologyId));
								}, (op3, error) -> {
									client(new RunTopoErrorSMsg(topologyId, error));
									node.topologyFault(topologyId,dragonTopology);
								}).onRunning((op3) -> {
									progress("starting topology on each daemon");
									try {
										node.startTopology(topologyId);
										((StartTopoGroupOp) op3).receiveSuccess(comms.getMyNodeDesc());
									} catch (DragonTopologyException | DragonInvalidStateException e) {
										((StartTopoGroupOp) op3).receiveError(comms.getMyNodeDesc(),e.getMessage());
										((StartTopoGroupOp)op3).fail(e.getMessage());
									}
								}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(),
										TimeUnit.MILLISECONDS, (op4)->{
									op4.fail("timed out starting the topology");
								});
							} catch (DragonInvalidContext e) {
								client(new RunTopoErrorSMsg(topologyId, e.getMessage()));
							}
						}, (op2, error) -> {
							client(new RunTopoErrorSMsg(topologyId, error));
						}).onRunning((op2) -> {
							progress("allocating topology on each daemon");
							try {
								node.prepareTopology(topologyId, conf, topo, false);
								((PrepTopoGroupOp) op2).receiveSuccess(comms.getMyNodeDesc());
							} catch (DragonRequiresClonableException | DragonTopologyException e) {
								((PrepTopoGroupOp) op2).receiveError(comms.getMyNodeDesc(), 
										e.getMessage());
							}
						}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(),
								TimeUnit.MILLISECONDS, (op3)->{
							op3.fail("timed out preparing the topology");		
						});
					} catch (DragonInvalidContext e) {
						client(new RunTopoErrorSMsg(topologyId, e.getMessage()));
					}
				}, (op, error) -> {
					client(new RunTopoErrorSMsg(topologyId, error));
				}).onRunning((op) -> {
					progress("distributing topology jar file");
					((PrepJarGroupOp) op).receiveSuccess(comms.getMyNodeDesc());
				}).onTimeout(node.getTimer(),node.getConf().getDragonServiceTimeoutMs(),TimeUnit.MILLISECONDS,(op)->{
					op.fail("timed out distributing the topology");
				});
			} catch (DragonInvalidContext e) {
				client(new RunTopoErrorSMsg(topologyId, e.getMessage()));
			}

		}
	}
	
}
