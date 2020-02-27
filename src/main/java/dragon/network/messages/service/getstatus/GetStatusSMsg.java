package dragon.network.messages.service.getstatus;

import java.util.concurrent.TimeUnit;

import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeStatus;
import dragon.network.comms.IComms;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.operations.GetStatusGroupOp;
import dragon.network.operations.Ops;

/**
 * Get the status of the dragon daemons
 * 
 * @author aaron
 *
 */
public class GetStatusSMsg extends ServiceMessage {
	private static final long serialVersionUID = 2277625239487077398L;

	public GetStatusSMsg() {
		super(ServiceMessage.ServiceMessageType.GET_STATUS);
	}

	/**
	 *
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		final IComms comms = node.getComms();
		Ops.inst().newGetStatusGroupOp((op)->{
			progress("waiting up to ["+node.getConf().getDragonServiceTimeoutMs()/1000+"] seconds...");
		},(op)->{
			GetStatusGroupOp gsgo = (GetStatusGroupOp) op;
			client(new StatusSMsg(gsgo.dragonStatus));
		}, (op,error)->{
			client(new GetStatusErrorSMsg((String)error));
		}).onRunning((op)->{
			GetStatusGroupOp gsgo = (GetStatusGroupOp) op;
			NodeStatus nodeStatus = node.getStatus();
			nodeStatus.context=new NodeContext();
			nodeStatus.context.putAll(node.getNodeProcessor().getAliveContext());
			gsgo.aggregate(nodeStatus);
			gsgo.receiveSuccess(comms.getMyNodeDesc());
		}).onTimeout(node.getTimer(), node.getConf().getDragonServiceTimeoutMs(), TimeUnit.MILLISECONDS, (op)->{
			op.fail("timed out waiting for nodes to respond");
		});
	}
}
