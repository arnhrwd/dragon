package dragon.network;

import java.util.ArrayList;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node.NodeState;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.context.ContextUpdateNMsg;
import dragon.network.messages.node.fault.RipNMsg;
import dragon.network.operations.Ops;


/**
 * Process node message, which are messages that can only originate from
 * other nodes (daemons). Reads nodes messages from the Comms layer, one
 * at a time.
 * @author aaron
 *
 */
public class NodeMsgProcessor extends Thread {
	private final static Logger log = LogManager.getLogger(NodeMsgProcessor.class);
	
	/**
	 * The node that this node processor belongs to.
	 */
	private final Node node;
	
	/**
	 * The set of node descriptors that this node believes are alive.
	 */
	private final NodeContext alive;
	
	/**
	 * The set of node descriptors that this node believes are dead.
	 */
	private final NodeContext dead;
	
	/**
	 * 
	 * @param node
	 */
	public NodeMsgProcessor() {
		this.node=Node.inst();
		alive=new NodeContext();
		dead=new NodeContext();
		alive.put(node.getComms().getMyNodeDesc());
		setName("node processor");
		start();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		log.info("starting up");
		while(!isInterrupted()) {
			NodeMessage msg;
			try {
				msg = node.getComms().receiveNodeMsg();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			log.debug("received ["+msg.getType().name()+"] from ["+msg.getSender());
			if(!alive.containsKey(msg.getSender().toString())&&msg.getType()!=NodeMessage.NodeMessageType.CONTEXT_UPDATE){
				log.warn("sender is not alive, so dropping message");
				Ops.inst().newOp((op)->{
					try {
						Node.inst().getComms().sendNodeMsg(msg.getSender(), new RipNMsg());
					} catch (DragonCommsException e) {
						op.fail("["+msg.getSender()+"] is really dead");
					}
				}, (op)->{
					op.success();
				}, (op)->{
					log.info("sent RIP to ["+msg.getSender()+"]");
				}, (op,error)->{
					log.warn(error);
				});
				continue;
			}
			if(msg.getGroupOp()!=null) {
				/*
				 * hook up our comms for sending replies.
				 */
				msg.getGroupOp().setComms(node.getComms());
			}
			
			// do when appropriate
			node.getOpsProcessor().newConditionOp((op)->{
				return node.getNodeState()==NodeState.OPERATIONAL;
			},(op)->{
				try {
					node.getOperationsLock().lockInterruptibly();
					msg.process();
				} catch (InterruptedException e) {
					log.error("interrupted while waiting for node operations lock");
				} finally {
					node.getOperationsLock().unlock();
				}
			}, (op,error)->{
				log.error(error);
			});
		}
		log.info("shutting down");
	}
	
	/**
	 * 
	 * @return the node context
	 */
	public NodeContext getAliveContext() {
		return alive;
	}
	
	private void setDeadnodeTimeout(NodeDescriptor desc) {
		node.getTimer().schedule(new TimerTask() {
			@Override
			public void run() {
				Ops.inst().newOp((op)->{
					if(alive.containsKey(desc.toString())) {
						op.cancel();
						return;
					}
					try {
						Node.inst().getComms().sendNodeMsg(desc, new ContextUpdateNMsg(alive));
					} catch (DragonCommsException e) {
						op.fail("["+desc+"] is still not reachable");
					}
				}, (op)->{
					op.success();
				}, (op)->{
					log.info("sent context update to ["+desc+"]");
				}, (op,msg)->{
					log.warn(msg);
				});
				setDeadnodeTimeout(desc);
			}
		}, node.getConf().getDragonFaultsDeadnodeTimeout());
	}
	
	/**
	 * set the node to dead and set a timeout to retry connecting
	 * to the dead node
	 * @param desc
	 */
	public void setDead(NodeDescriptor desc) {
		if(alive.containsKey(desc.toString())) alive.remove(desc.toString());
		dead.put(desc.toString(),desc);
		setDeadnodeTimeout(desc);
	}
	
	/**
	 * set the node to alive
	 * @param desc
	 */
	public void setAlive(NodeDescriptor desc) {
		if(dead.containsKey(desc.toString())) dead.remove(desc.toString());
		alive.put(desc.toString(),desc);
	}
	
	/**
	 * Put all of the given context into the node's alive context.
	 * @param context
	 */
	public synchronized void contextPutAll(NodeContext context) {
		context.forEach((k,v)->{
			if(dead.containsKey(k)) dead.remove(k);
			alive.put(k,v);
		});
	}

	/**
	 * 
	 */
	public void setAllDead() {
		ArrayList<NodeDescriptor> c = new ArrayList<>(alive.values());
		c.forEach((desc)->{
			if(alive.containsKey(desc.toString())) alive.remove(desc.toString());
			dead.put(desc.toString(),desc);
		});
	}
}
