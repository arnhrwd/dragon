package dragon.network.messages.service;

import dragon.network.messages.Message;

public class ServiceMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -682715214185176661L;
	
	public static enum ServiceMessageType {
		RUN_TOPOLOGY,
		TOPOLOGY_EXISTS,
		TOPOLOGY_SUBMITTED
	}
	
	private ServiceMessageType type;
	
	public ServiceMessage(ServiceMessageType type){
		this.type=type;
	}
	
	public ServiceMessageType getType(){
		return type;
	}
	
}
