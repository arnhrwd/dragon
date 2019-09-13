package dragon.network.messages.service;

public class TopologyErrorMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 599535561764460099L;
	public String topologyName;
	public String error;
	public TopologyErrorMessage(String name, String error) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_ERROR);
		this.topologyName=name;
		this.error = error;
	}
	
}
