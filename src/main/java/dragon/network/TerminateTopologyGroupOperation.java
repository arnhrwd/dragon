package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.StopTopologyErrorMessage;
import dragon.network.messages.node.StopTopologyMessage;
import dragon.network.messages.node.TopologyStoppedMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.TopologyTerminatedMessage;

public class TerminateTopologyGroupOperation extends GroupOperation {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7596391746339394369L;
	private static Log log = LogFactory.getLog(TerminateTopologyGroupOperation.class);
	String topologyId;
	public TerminateTopologyGroupOperation(TerminateTopologyMessage ttm) {
		super(ttm);
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
	
	@Override
	protected ServiceMessage successServiceMessage() {
		return new TopologyTerminatedMessage(topologyId);
	}
	
	@Override
	protected ServiceMessage failServiceMessage(String error) {
		return new TerminateTopologyErrorMessage(topologyId,error);
	}
}
