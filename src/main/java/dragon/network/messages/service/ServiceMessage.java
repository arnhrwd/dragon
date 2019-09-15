package dragon.network.messages.service;

import dragon.network.messages.Message;

public class ServiceMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -682715214185176661L;
	
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
		TOPOLOGY_TERMINATED
	}
	
	private ServiceMessageType type;
	
	public ServiceMessage(ServiceMessageType type){
		this.type=type;
	}
	
	public ServiceMessageType getType(){
		return type;
	}
	
}
