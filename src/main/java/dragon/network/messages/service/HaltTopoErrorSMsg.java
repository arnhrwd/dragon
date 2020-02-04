package dragon.network.messages.service;

/**
 * Error attempting to halt the topology
 * @author aaron
 *
 */
public class HaltTopoErrorSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = -7506239870047998404L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public HaltTopoErrorSMsg(String topologyId, String error) {
		super(ServiceMessage.ServiceMessageType.HALT_TOPOLOGY_ERROR,error);
		this.topologyId=topologyId;
	}

}
