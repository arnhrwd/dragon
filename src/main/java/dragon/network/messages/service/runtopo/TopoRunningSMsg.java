package dragon.network.messages.service.runtopo;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class TopoRunningSMsg extends ServiceMessage {
	private static final long serialVersionUID = -2760522646650784474L;
	
	/**
	 * 
	 */
	public String topologyName;
	
	/**
	 * @param topologyName
	 */
	public TopoRunningSMsg(String topologyName) {
		super(ServiceMessage.ServiceMessageType.TOPOLOGY_RUNNING);
		this.topologyName = topologyName;
	}

}
