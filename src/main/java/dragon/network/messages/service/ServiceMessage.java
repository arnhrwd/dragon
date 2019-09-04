package dragon.network.messages.service;

import dragon.network.messages.Message;

public class ServiceMessage extends Message {
	/**
	 * 
	 */
	private static final long serialVersionUID = -682715214185176661L;
	
	public static enum ServiceCommandType {
		RUN_TOPOLOGY,
		TOPOLOGY_EXISTS,
		TOPOLOGY_SUBMITTED
	}
	
	private ServiceCommandType type;
	
	public ServiceMessage(ServiceCommandType type){
		this.type=type;
	}
	
	public ServiceCommandType getType(){
		return type;
	}
	
}
