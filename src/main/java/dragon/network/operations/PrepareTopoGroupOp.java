package dragon.network.operations;


import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareTopoErrorNMsg;
import dragon.network.messages.node.PrepareTopoNMsg;
import dragon.network.messages.node.TopoReadyNMsg;
import dragon.network.messages.service.RunTopoSMsg;


public class PrepareTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 7223966055440319387L;
	private RunTopoSMsg rtm;
	
	public PrepareTopoGroupOp(RunTopoSMsg orig,IOpSuccess success,
			IOpFailure failure) {
		super(success,failure);
		this.rtm=orig;
	}

	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareTopoNMsg(rtm.topologyId,rtm.conf,rtm.dragonTopology);
	}

	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoReadyNMsg(rtm.topologyId);
	}

	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new PrepareTopoErrorNMsg(rtm.topologyId, error);
	}

}
