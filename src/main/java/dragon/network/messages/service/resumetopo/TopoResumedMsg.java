package dragon.network.messages.service.resumetopo;

import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class TopoResumedMsg extends ServiceMessage {
	private static final long serialVersionUID = -8899838211586131880L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoResumedMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_RESUMED);
		this.topologyId=topologyId;
	}

}
