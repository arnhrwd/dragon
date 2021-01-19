package dragon.network.messages.service.halttopo;

import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class TopoHaltedSMsg extends ServiceMessage {
	private static final long serialVersionUID = 989132255201705121L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 */
	public TopoHaltedSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_HALTED);
		this.topologyId=topologyId;
	}

}
