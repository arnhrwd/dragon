package dragon.network.operations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StopTopologyErrorMessage;
import dragon.network.messages.node.StopTopologyMessage;
import dragon.network.messages.node.TopologyStoppedMessage;
import dragon.network.messages.service.TerminateTopologyMessage;


public class TerminateTopologyGroupOperation extends GroupOperation {
	private static final long serialVersionUID = -7596391746339394369L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(TerminateTopologyGroupOperation.class);
	private final String topologyId;
	
	public TerminateTopologyGroupOperation(TerminateTopologyMessage ttm,IOperationSuccess success, IOperationFailure failure) {
		super(ttm,success,failure);
		this.topologyId=ttm.topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new StopTopologyMessage(topologyId);
	}
	
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopologyStoppedMessage(topologyId);
	}
	
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new StopTopologyErrorMessage(topologyId,error);
	}

}
