package dragon.network.messages.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
import dragon.network.comms.DragonCommsException;
import dragon.network.comms.IComms;
import dragon.network.messages.Message;
import dragon.network.messages.service.progress.ProgressSMsg;

public class ServiceMessage extends Message {
	private static final long serialVersionUID = -682715214185176661L;
	private final static Logger log = LogManager.getLogger(ServiceMessage.class);
	
	/**
	 * The client id, used to identify the client to respond to.
	 */
	private String messageId="";
	
	/**
	 * @author aaron
	 *
	 */
	public static enum ServiceMessageType {
		RUN_TOPOLOGY,
		TOPOLOGY_ERROR,
		TOPOLOGY_RUNNING,
		GET_NODE_CONTEXT,
		NODE_CONTEXT,
		SERVICE_DONE,
		RUN_TOPOLOGY_ERROR,
		GET_METRICS,
		METRICS,
		GET_METRICS_ERROR,
		UPLOAD_JAR,
		UPLOAD_JAR_SUCCESS,
		TERMINATE_TOPOLOGY,
		UPLOAD_JAR_FAILED,
		TERMINATE_TOPOLOGY_ERROR,
		TOPOLOGY_TERMINATED,
		LIST_TOPOLOGIES,
		LIST_TOPOLOGIES_ERROR,
		TOPOLOGY_LIST,
		HALT_TOPOLOGY,
		TOPOLOGY_HALTED,
		HALT_TOPOLOGY_ERROR,
		RESUME_TOPOLOGY,
		TOPOLOGY_RESUMED,
		RESUME_TOPOLOGY_ERROR,
		ALLOCATE_PARTITION,
		PARTITION_ALLOCATED,
		ALLOCATE_PARTITION_ERROR,
		GET_STATUS,
		GET_STATUS_ERROR,
		STATUS,
		DEALLOCATE_PARTITION,
		DEALLOCATE_PARTITION_ERROR,
		PARTITION_DEALLOCATED,
		PROGRESS,
		TOPOLOGY_ID,
		EXEC_RL_ACTION, 
		RL_ACTION_EXECUTED, 
		EXEC_RL_ACTION_ERROR
	}
	
	/**
	 * 
	 */
	private ServiceMessageType type;
	
	/**
	 * @param type
	 */
	public ServiceMessage(ServiceMessageType type){
		this.type=type;
	}
	
	/**
	 * @return
	 */
	public ServiceMessageType getType(){
		return type;
	}
	
	/**
	 * @param messageId
	 */
	public void setMessageId(String messageId) {
		this.messageId=messageId;
	}
	
	/**
	 * @return
	 */
	public String getMessageId() {
		return messageId;
	}
	
	/**
	 * Utility function to send a message to the client.
	 * @param msg
	 * @return true if it was sent, false otherwise
	 */
	public boolean client(ServiceMessage msg) {
		final IComms comms = Node.inst().getComms();
		try {
			comms.sendServiceMsg(msg, this);
		} catch (DragonCommsException e) {
			log.fatal("can't communicate with client: " + e.getMessage());
			return false;
		}
		return true;
	}
	
	/**
	 * Utility function to report progress back to client.
	 * @param msg is shown to the user at the client
	 */
	public boolean progress(String msg) {
		return client(new ProgressSMsg(msg));
	}
	
}
