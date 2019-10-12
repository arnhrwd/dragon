package dragon.network.operations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StopTopologyErrorMessage;
import dragon.network.messages.node.StopTopologyMessage;
import dragon.network.messages.node.TopologyStoppedMessage;
import dragon.network.messages.service.TerminateTopologyMessage;


public class TermTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -7596391746339394369L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(TermTopoGroupOp.class);
	private final String topologyId;
	
	public TermTopoGroupOp(TerminateTopologyMessage ttm,IOpSuccess success, IOpFailure failure) {
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
