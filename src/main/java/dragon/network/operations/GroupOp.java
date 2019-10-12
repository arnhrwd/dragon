package dragon.network.operations;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeDescriptor;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.messages.Message;
import dragon.network.messages.node.NodeMessage;

/**
 * A group operation is one which sends a number of daemons messages and waits
 * for them all to respond, either as success or error. A single error generates
 * a failure outcome, while all in success generates a success outcome.
 * @author aaron
 *
 */
public class GroupOp extends Op implements Serializable {
	private static final long serialVersionUID = 7500196228211761411L;
	private static Log log = LogFactory.getLog(GroupOp.class);
	protected transient HashSet<NodeDescriptor> group;
	
	private Message orig;
	
	public GroupOp(Message orig,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		this.orig = orig;
		group = new HashSet<NodeDescriptor>();
	}
	
	public void add(NodeDescriptor desc) {
		group.add(desc);
	}
	
	protected boolean remove(NodeDescriptor desc) {
		group.remove(desc);
		return group.isEmpty();
	}

	private void sendGroupNodeMessage(IComms comms,NodeDescriptor desc, NodeMessage message, Message to) throws DragonCommsException {
		message.setGroupOperation(this);
		comms.sendNodeMessage(desc,message,to);
	}

	public void initiate(IComms comms) {
		super.start();
		for(NodeDescriptor desc : group) {
			if(!desc.equals(getSourceDesc())) {
				try {
					sendGroupNodeMessage(comms,desc, initiateNodeMessage() ,orig);
				} catch (DragonCommsException e) {
					fail("network errors prevented group operation");
				}
			}
		}
	}
	
	public void sendSuccess(IComms comms) {
		if(!getSourceDesc().equals(comms.getMyNodeDescriptor())) {
			NodeMessage tsm = successNodeMessage();
			try {
				sendGroupNodeMessage(comms,getSourceDesc(), tsm, orig);
			} catch (DragonCommsException e) {
				log.fatal("network errors prevented group operation");
			}
		} else {
			receiveSuccess(comms,comms.getMyNodeDescriptor());
		}
	}
	
	public void sendError(IComms comms,String error) {
		try {
			comms.sendNodeMessage(getSourceDesc(),errorNodeMessage(error),orig);
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
	 * Appropriate message are provided by the subclass.
	 */

	protected NodeMessage initiateNodeMessage() {
		return null;
	}
	protected NodeMessage successNodeMessage() {
		return null;
	}
	protected NodeMessage errorNodeMessage(String error) {
		return null;
	}

}
