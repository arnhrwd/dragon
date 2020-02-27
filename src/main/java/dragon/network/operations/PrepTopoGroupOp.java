package dragon.network.operations;


import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.preparetopo.PrepareTopoErrorNMsg;
import dragon.network.messages.node.preparetopo.PrepareTopoNMsg;
import dragon.network.messages.node.preparetopo.TopoReadyNMsg;
import dragon.network.messages.service.runtopo.RunTopoSMsg;


/**
 * @author aaron
 *
 */
public class PrepTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = 7223966055440319387L;
	
	/**
	 * 
	 */
	private RunTopoSMsg rtm;
	
	/**
	 * @param orig
	 * @param success
	 * @param failure
	 */
	public PrepTopoGroupOp(IComms comms,RunTopoSMsg orig,IOpSuccess success,
			IOpFailure failure) {
		super(comms,success,failure);
		this.rtm=orig;
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new PrepareTopoNMsg(rtm.topologyId,rtm.conf,rtm.dragonTopology);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		final String topologyId=rtm.topologyId;
		rtm=null;
		return new TopoReadyNMsg(topologyId);
	}

	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		final String topologyId=rtm.topologyId;
		rtm=null;
		return new PrepareTopoErrorNMsg(topologyId, error);
	}

}
