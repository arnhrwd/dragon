package dragon.network.messages.node.gettopoinfo;

import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;
import dragon.network.operations.ListToposGroupOp;

/**
 * @author aaron
 *
 */
public class GetTopoInfoNMsg extends NodeMessage {
	private static final long serialVersionUID = 1383319162954166063L;
	
	/**
	 * 
	 */
	public GetTopoInfoNMsg() {
		super(NodeMessage.NodeMessageType.GET_TOPOLOGY_INFORMATION);
	}
	
	/**
	 * 
	 */
	@Override
	public void process() {
		final Node node = Node.inst();
		ListToposGroupOp ltgo = (ListToposGroupOp)getGroupOp();
		node.listTopologies(ltgo); 
		ltgo.sendSuccess();
	}
}
