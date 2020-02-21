package dragon.network.operations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
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
	
	/**
	 * 
	 */
	private static final Logger log = LogManager.getLogger(GroupOp.class);
	
	/**
	 * The group of nodes that will be sent an initiate message when the
	 * group operation starts.
	 */
	protected transient final HashSet<NodeDescriptor> group; // not necessary at the group members
	
	/**
	 * The messages received from each of the group members. Will not contain
	 * a message from the group member if it is the same node as initiating the
	 * group operation.
	 */
	protected transient final ArrayList<NodeMessage> received; // not necessary at the group members
	
	/**
	 * Comms for this group op. Needs to be set at the receiver
	 * to the receiver's comms.
	 */
	protected transient IComms comms;
	
	/**
	 * @param success
	 * @param failure
	 */
	public GroupOp(IComms comms,IOpSuccess success,IOpFailure failure) {
		super(success,failure);
		group = new HashSet<NodeDescriptor>();
		received=new ArrayList<NodeMessage>();
		this.comms=comms;
	}
	
	public void setComms(IComms comms) {
		this.comms=comms;
	}
	
	/**
	 * @param desc
	 */
	public void add(NodeDescriptor desc) {
		group.add(desc);
	}
	
	/**
	 * @param desc
	 * @return
	 */
	protected boolean remove(NodeDescriptor desc) {
		group.remove(desc);
		return group.isEmpty();
	}

	/**
	 * @param comms
	 * @param desc
	 * @param message
	 * @throws DragonCommsException
	 */
	private void sendGroupNodeMessage(IComms comms,
			NodeDescriptor desc, NodeMessage message) 
					throws DragonCommsException {
		message.setGroupOp(this);
		comms.sendNodeMsg(desc,message);
	}

	/**
	 * @throws Exception 
	 * 
	 */
	@Override
	public void start() {
		ArrayList<NodeDescriptor> descs = new ArrayList<>();
		descs.addAll(group);
		sendStartMessages(descs);
	}
	
	public void sendStartMessages(List<NodeDescriptor> descs) {
		if(descs.size()==0) {
			log.debug("calling super start");
			super.start();
		} else {
			NodeDescriptor desc=descs.remove(0);
			log.debug("considering "+desc);
			/*
			* The thread that initiated the group op needs to make sure
			* that it is resolved for the source desc.
			*/
			if(desc.equals(getSourceDesc())) {
				sendStartMessages(descs);
			} else {
				// otherwise we send a message
				Ops.inst().newOp((op)->{
					try {
						sendGroupNodeMessage(comms,desc, initiateNodeMessage(desc));
					} catch (DragonCommsException e) {
						op.fail("network errors prevented group operation");
						Node.inst().nodeFault(desc);
					}
				}, (op)->{
					log.debug("sent message to "+desc);
					op.success();
				}, (op)->{
					sendStartMessages(descs);
				}, (op,msg)->{
					fail(msg);
				});
			}
		}
	}
	
	/**
	 * Send a success message back to the source of the group op.
	 * If the source desc of the group op is the same as the local
	 * node's desc, then calling this method is a proxy for calling
	 * {@link #receiveSuccess(IComms, NodeDescriptor)}.
	 * @param comms
	 */
	public void sendSuccess() {
		if(!getSourceDesc().equals(comms.getMyNodeDesc())) {
			NodeMessage tsm = successNodeMessage();
			try {
				sendGroupNodeMessage(comms,getSourceDesc(), tsm);
			} catch (DragonCommsException e) {
				log.fatal("network errors prevented group operation");
				Node.inst().nodeFault(getSourceDesc());
			}
		} else {
			receiveSuccess(comms.getMyNodeDesc());
		}
	}
	
	/**
	 * Send an error message back to the source of the group op.
	 * If the source desc of the group op is the same as the local
	 * node's desc, then calling this method is a proxy for calling
	 * {@link #receiveError(IComms, NodeDescriptor, String)}.
	 * @param comms
	 * @param error
	 */
	public void sendError(String error) {
		if(!getSourceDesc().equals(comms.getMyNodeDesc())) {
			try {
				comms.sendNodeMsg(getSourceDesc(),errorNodeMessage(error));
			} catch (DragonCommsException e) {
				log.fatal("network errors prevented group operation");
				Node.inst().nodeFault(getSourceDesc());
			}
		} else {
			receiveError(comms.getMyNodeDesc(),error);
		}
	}
	
	/**
	 * @param comms
	 * @param msg
	 */
	public void receiveSuccess(NodeMessage msg) {
		received.add(0,msg);
		receiveSuccess(msg.getSender());
	}
	
	/**
	 * @param comms
	 * @param desc
	 */
	public void receiveSuccess(NodeDescriptor desc) {
		if(remove(desc)) {
			success();
		}
	}
	
	/**
	 * @param comms
	 * @param msg
	 * @param error
	 */
	public void receiveError(NodeMessage msg, String error) {
		received.add(0,msg);
		receiveError(msg.getSender(),error);
	}
	
	/**
	 * @param comms
	 * @param desc
	 * @param error
	 */
	public void receiveError(NodeDescriptor desc,String error) {
		remove(desc);
		fail(error);
	}
	
	/**
	 * @return
	 */
	public ArrayList<NodeMessage> getReceived() {
		return received;
	}
	
	/*
	 * Appropriate message MUST be provided by the subclass, if used.
	 * In some cases, error conditions may never arise and so the error
	 * message need not be provided.
	 */

	/**
	 * @param desc
	 * @return
	 */
	protected abstract NodeMessage initiateNodeMessage(NodeDescriptor desc);
	
	/**
	 * @return
	 */
	protected abstract NodeMessage successNodeMessage();
	
	/**
	 * @param error
	 * @return
	 */
	protected abstract NodeMessage errorNodeMessage(String error);

}
