package dragon.network.messages.service.runtopo;

import dragon.network.messages.service.ServiceMessage;

/**
 * 
 * @author aaron
 *
 */
public class TopologyIDSMsg extends ServiceMessage {
	private static final long serialVersionUID = 1294142036953726071L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 * @param topologyId
	 */
	public TopologyIDSMsg(String topologyId) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_ID);
		this.topologyId=topologyId;
	}

}
