package dragon.network.operations;

import dragon.network.messages.node.JarReadyMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorMessage;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.service.RunTopologyMessage;

public class RunTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -2038551040445600017L;
	private RunTopologyMessage rtm;
	private transient byte[] jar;
	
	public RunTopoGroupOp(RunTopologyMessage orig,byte[] jar,IOpSuccess success,
			IOpFailure failure) {
		super(arbridged(orig),success,failure);
		this.rtm=orig;
		this.rtm.dragonTopology=null;
		this.jar=jar;
	}
	
	private static RunTopologyMessage arbridged(RunTopologyMessage rtm) {
		RunTopologyMessage rtmlocal = new RunTopologyMessage(rtm.topologyName,rtm.conf,null);
		rtmlocal.setMessageId(rtm.getMessageId());
		return rtmlocal;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareJarMessage(rtm.topologyName,jar);
	}
	@Override
	protected NodeMessage successNodeMessage() {
		return new JarReadyMessage(rtm.topologyName);
	}
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new PrepareJarErrorMessage(rtm.topologyName,error);
	}

}
