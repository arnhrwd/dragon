package dragon.network.operations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.stoptopo.TermTopoErrorNMsg;
import dragon.network.messages.node.stoptopo.TermTopoNMsg;
import dragon.network.messages.node.stoptopo.TopoTermdNMsg;


/**
 * @author aaron
 *
 */
public class TermTopoGroupOp extends GroupOp {
	private static final long serialVersionUID = -7596391746339394369L;
	
	/**
	 * 
	 */
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(TermTopoGroupOp.class);
	
	/**
	 * 
	 */
	private final String topologyId;
	
	/**
	 * @param topologyId
	 * @param success
	 * @param failure
	 */
	public TermTopoGroupOp(IComms comms,String topologyId,IOpStart start,IOpSuccess success, IOpFailure failure) {
		super(comms,start,success,failure);
		this.topologyId=topologyId;
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#initiateNodeMessage(dragon.network.NodeDescriptor)
	 */
	@Override
	protected NodeMessage initiateNodeMessage(NodeDescriptor desc) {
		return new TermTopoNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#successNodeMessage()
	 */
	@Override
	protected NodeMessage successNodeMessage() {
		return new TopoTermdNMsg(topologyId);
	}
	
	/* (non-Javadoc)
	 * @see dragon.network.operations.GroupOp#errorNodeMessage(java.lang.String)
	 */
	@Override
	protected NodeMessage errorNodeMessage(String error) {
		return new TermTopoErrorNMsg(topologyId,error);
	}

}
