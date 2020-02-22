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
			case CONTEXT_UPDATE:
			case GET_TOPOLOGY_INFORMATION:
			case HALT_TOPOLOGY:
			case HALT_TOPOLOGY_ERROR:
			case JAR_READY:
			case JOIN_REQUEST:
			case PREPARE_JAR:
			case PREPARE_JAR_ERROR:
			case PREPARE_TOPOLOGY:
			case PREPARE_TOPOLOGY_ERROR:
			case REMOVE_TOPOLOGY:
			case REMOVE_TOPOLOGY_ERROR:
			case RESUME_TOPOLOGY:
			case RESUME_TOPOLOGY_ERROR:
			case START_TOPOLOGY:
			case START_TOPOLOGY_ERROR:
			case TERMINATE_TOPOLOGY:
			case TERMINATE_TOPOLOGY_ERROR:
			case TOPOLOGY_HALTED:
			case TOPOLOGY_INFORMATION:
			case TOPOLOGY_READY:
			case TOPOLOGY_REMOVED:
			case TOPOLOGY_RESUMED:
			case TOPOLOGY_STARTED:
			case TOPOLOGY_TERMINATED:
			case ALLOCATE_PARTITION:
			case ALLOCATE_PARTITION_ERROR:
			case PARTITION_ALLOCATED:
			case GET_STATUS:
			case GET_STATUS_ERROR:
			case STATUS:
			case DEALLOCATE_PARTITION:
			case DEALLOCATE_PARTITION_ERROR:
			case PARTITION_DEALLOCATED:
			case TERMINATE_NODE:
			case NODE_FAULT:
			case TOPOLOGY_FAULT:
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
			default:
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
