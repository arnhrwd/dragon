package dragon.network.operations;

import dragon.network.messages.node.JarReadyNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorNMsg;
import dragon.network.messages.node.PrepareJarNMsg;
import dragon.network.messages.service.RunTopoSMsg;

public class RunTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -2038551040445600017L;
	private RunTopoSMsg rtm;
	private transient byte[] jar;
	
	public RunTopoGroupOp(RunTopoSMsg orig,byte[] jar,IOpSuccess success,
			IOpFailure failure) {
		super(arbridged(orig),success,failure);
		this.rtm=orig;
		this.rtm.dragonTopology=null;
		this.jar=jar;
	}
	
	private static RunTopoSMsg arbridged(RunTopoSMsg rtm) {
		RunTopoSMsg rtmlocal = new RunTopoSMsg(rtm.topologyName,rtm.conf,null);
		rtmlocal.setMessageId(rtm.getMessageId());
		return rtmlocal;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareJarNMsg(rtm.topologyName,jar);
	}
	@Override
	protected NodeMessage successNodeMessage() {
		return new JarReadyNMsg(rtm.topologyName);
	}
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new PrepareJarErrorNMsg(rtm.topologyName,error);
	}

}
