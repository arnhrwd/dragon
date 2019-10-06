package dragon.network.messages.service;

public class HaltTopologyErrorMessage extends ServiceMessage {
	private static final long serialVersionUID = -7506239870047998404L;
	public final String topologyId;
	public final String error;
	public HaltTopologyErrorMessage(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.HALT_TOPOLOGY_ERROR);
		this.topologyId=topologyId;
		this.error=error;
	}

}
