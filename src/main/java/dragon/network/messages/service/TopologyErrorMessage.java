package dragon.network.messages.service;

public class TopologyErrorMessage extends ServiceMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = 950825340997670248L;
	public String topologyName;
	public String error;
	public TopologyErrorMessage(String name, String error) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_ERROR);
		topologyName=name;
		this.error = error;
	}
	
}
