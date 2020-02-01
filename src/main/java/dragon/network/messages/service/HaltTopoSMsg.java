package dragon.network.messages.service;

/**
 * @author aaron
 *
 */
public class HaltTopoSMsg extends ServiceMessage {
	private static final long serialVersionUID = 4205778402629198684L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public HaltTopoSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.HALT_TOPOLOGY);
		this.topologyId=topologyId;
	}

}
