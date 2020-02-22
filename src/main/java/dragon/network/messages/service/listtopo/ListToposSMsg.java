	package dragon.network.messages.service.listtopo;

import java.util.concurrent.TimeUnit;

import dragon.network.Node;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.ListToposGroupOp;
import dragon.network.operations.Ops;

/**
 * List general information for all topologies running on all daemons.
 * @author aaron
 *
 */
public class ListToposSMsg extends ServiceMessage {
	private static final long serialVersionUID = -8279553169106206333L;
	
	/**
	 * 
	 */
	public ListToposSMsg() {
		super(ServiceMessage.ServiceMessageType.LIST_TOPOLOGIES);
	}

	/**
	 *
	 */
	@Override
	public void process() {
		final Node node=Node.inst();
		final IComms comms = node.getComms();
		Ops.inst().newListToposGroupOp((op)->{
			progress("waiting up to ["+Node.inst().getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
		},(op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			client(new TopoListSMsg(ltgo.descState, ltgo.descErrors, ltgo.descComponents, ltgo.descMetrics));
		}, (op, error) -> {
			client(new ListToposErrorSMsg(error));
		}).onRunning((op) -> {
			ListToposGroupOp ltgo = (ListToposGroupOp) op;
			node.listTopologies(ltgo);
			ltgo.aggregate(comms.getMyNodeDesc(), ltgo.state, ltgo.errors, ltgo.components, ltgo.metrics);
			ltgo.receiveSuccess(comms.getMyNodeDesc());
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}
}
