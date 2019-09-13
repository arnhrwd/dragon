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
		TOPOLOGY_SUBMITTED,
		GET_NODE_CONTEXT,
		NODE_CONTEXT,
		SERVICE_DONE,
		RUN_FAILED,
		GET_METRICS,
		METRICS,
		METRICS_ERROR,
		JARFILE,
		JARFILE_STORED,
		TERMINATE_TOPOLOGY
	}
	
	private ServiceMessageType type;
	
	public ServiceMessage(ServiceMessageType type){
		this.type=type;
	}
	
	public ServiceMessageType getType(){
		return type;
	}
	
}
