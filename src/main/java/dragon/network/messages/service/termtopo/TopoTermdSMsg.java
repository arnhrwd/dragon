package dragon.network.messages.service.termtopo;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class TopoTermdSMsg extends ServiceMessage {
	private static final long serialVersionUID = -4461190987886019247L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoTermdSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_TERMINATED);
		this.topologyId=topologyId;
	}

}
