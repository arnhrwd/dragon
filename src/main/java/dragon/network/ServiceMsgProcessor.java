package dragon.network;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node.NodeState;
import dragon.network.messages.service.ServiceMessage;


/**
 * Process service messages that come from a client. Processing may require
 * issuing a sequence of group operations. This object will read service
 * messages one at time from the Comms layer.
 * 
 * @author aaron
 *
 */
public class ServiceMsgProcessor extends Thread {
	private final static Logger log = LogManager.getLogger(ServiceMsgProcessor.class);
	
	/**
	 * Cached node reference.
	 */
	private final Node node;

	/**
	 * @param node
	 */
	public ServiceMsgProcessor() {
		this.node = Node.inst();
		setName("service processor");
		start();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		log.info("starting up");
		while (!isInterrupted()) {
			ServiceMessage msg;
			try {
				msg = node.getComms().receiveServiceMsg();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
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
}
