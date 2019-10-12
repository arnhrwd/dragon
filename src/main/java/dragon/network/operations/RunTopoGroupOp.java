package dragon.network.operations;

import dragon.network.messages.node.JarReadyNMsg;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarErrorNMsg;
import dragon.network.messages.node.PrepareJarNMsg;

public class RunTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -2038551040445600017L;
	private final transient byte[] jar;
	private final String topologyId;
	public RunTopoGroupOp(String topologyId,byte[] jar,IOpSuccess success,
			IOpFailure failure) {
		super(success,failure);
		this.jar=jar;
		this.topologyId=topologyId;
	}
	
	@Override
	protected NodeMessage initiateNodeMessage() {
		return new PrepareJarNMsg(topologyId,jar);
	}
	@Override
	protected NodeMessage successNodeMessage() {
		return new JarReadyNMsg(topologyId);
	}
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new PrepareJarErrorNMsg(topologyId,error);
	}

}
