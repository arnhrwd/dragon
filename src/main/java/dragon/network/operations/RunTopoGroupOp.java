package dragon.network.operations;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.preparejar.JarReadyNMsg;
import dragon.network.messages.node.preparejar.PrepareJarErrorNMsg;
import dragon.network.messages.node.preparejar.PrepareJarNMsg;

/**
 * @author aaron
 *
 */
public class RunTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -2038551040445600017L;
	
	/**
	 * 
	 */
	private final transient byte[] jar;
	
	/**
	 * 
	 */
	private final String topologyId;
	
	/**
	 * @param topologyId
	 * @param jar
	 * @param success
	 * @param failure
	 */
	public RunTopoGroupOp(String topologyId,byte[] jar,IOpSuccess success,
			IOpFailure failure) {
		super(success,failure);
		this.jar=jar;
		this.topologyId=topologyId;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new PrepareJarNMsg(topologyId,jar);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new JarReadyNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new PrepareJarErrorNMsg(topologyId,error);
	}

}
