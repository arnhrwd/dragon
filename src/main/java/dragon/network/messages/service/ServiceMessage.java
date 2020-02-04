package dragon.network.messages.service;

import dragon.network.messages.Message;

public class ServiceMessage extends Message {
	private static final long serialVersionUID = -682715214185176661L;
	
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
		STATUS
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
	
}
