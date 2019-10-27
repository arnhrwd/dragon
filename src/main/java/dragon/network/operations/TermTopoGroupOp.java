package dragon.network.operations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StopTopoErrorNMsg;
import dragon.network.messages.node.StopTopoNMsg;
import dragon.network.messages.node.TopoStoppedNMsg;


public class TermTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -7596391746339394369L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(TermTopoGroupOp.class);
	private final String topologyId;
	public TermTopoGroupOp(String topologyId,IOpSuccess success, IOpFailure failure) {
		super(success,failure);
		this.topologyId=topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new StopTopoNMsg(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoStoppedNMsg(topologyId);
	}
	
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new StopTopoErrorNMsg(topologyId,error);
	}

}
