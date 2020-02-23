package dragon.network;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node.NodeState;
import dragon.network.messages.node.NodeMessage;


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
	 * The set of node descriptors that this node processor knows about.
	 */
	private final NodeContext context;
	
	/**
	 * 
	 * @param node
	 */
	public NodeMsgProcessor() {
		this.node=Node.inst();
		context=new NodeContext();
		context.put(node.getComms().getMyNodeDesc());
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
			if(msg.getGroupOp()!=null) {
				/*
				 * hook up our comms for sending replies.
				 */
				msg.getGroupOp().setComms(node.getComms());
			}
			switch(msg.getType()) {
			case ACCEPTING_JOIN:
			case JOIN_COMPLETE:
				msg.process();
				break;
			default:
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
				break;
			}
		}
		log.info("shutting down");
	}
	
	/**
	 * 
	 * @return the node context
	 */
	public NodeContext getContext() {
		return context;
	}
	
	/**
	 * Put all of the given context into the node's context.
	 * @param context
	 */
	public synchronized void contextPutAll(NodeContext context) {
		this.context.putAll(context);
	}
}
