package dragon.network.operations;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeDescriptor;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.messages.node.NodeMessage;

/**
 * A group operation is one which sends a number of daemons messages and waits
 * for them all to respond, either as success or error. A single error generates
 * a failure outcome, while all in success generates a success outcome.
 * @author aaron
 *
 */
public abstract class GroupOp extends Op implements Serializable {
	private static final long serialVersionUID = 7500196228211761411L;
	private static final Log log = LogFactory.getLog(GroupOp.class);
	protected transient HashSet<NodeDescriptor> group; // not necessary at the group members
	
	public GroupOp(IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		group = new HashSet<NodeDescriptor>();
	}
	
	public void add(NodeDescriptor desc) {
		group.add(desc);
	}
	
	protected boolean remove(NodeDescriptor desc) {
		group.remove(desc);
		return group.isEmpty();
	}

	private void sendGroupNodeMessage(IComms comms,
			NodeDescriptor desc, NodeMessage message) 
					throws DragonCommsException {
		message.setGroupOp(this);
		comms.sendNodeMsg(desc,message);
	}

	public void initiate(IComms comms) {
		for(NodeDescriptor desc : group) {
			if(!desc.equals(getSourceDesc())) {
				try {
					sendGroupNodeMessage(comms,desc, initiateNodeMessage());
				} catch (DragonCommsException e) {
					fail("network errors prevented group operation");
				}
			}
		}
		super.start();
	}
	
	public void sendSuccess(IComms comms) {
		if(!getSourceDesc().equals(comms.getMyNodeDesc())) {
			NodeMessage tsm = successNodeMessage();
			try {
				sendGroupNodeMessage(comms,getSourceDesc(), tsm);
			} catch (DragonCommsException e) {
				log.fatal("network errors prevented group operation");
			}
		} else {
			receiveSuccess(comms,comms.getMyNodeDesc());
		}
	}
	
	public void sendError(IComms comms,String error) {
		try {
			comms.sendNodeMsg(getSourceDesc(),errorNodeMessage(error));
		} catch (DragonCommsException e) {
			log.fatal("network errors prevented group operation");
		}
	}
	
	public void receiveSuccess(IComms comms, NodeDescriptor desc) {
		if(remove(desc)) {
			success();
		}
	}
	
	public void receiveError(IComms comms, NodeDescriptor desc,String error) {
		remove(desc);
		fail(error);
	}
	
	/*
	 * Appropriate message MUST be provided by the subclass, if used.
	 * In some cases, error conditions may never arise and so the error
	 * message need not be provided.
	 */

	protected abstract NodeMessage initiateNodeMessage();
	protected abstract NodeMessage successNodeMessage();
	protected abstract NodeMessage errorNodeMessage(String error);

}
