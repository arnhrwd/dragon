package dragon.network.messages.service.progress;

import dragon.network.messages.service.ServiceMessage;

/**
 * 
 * @author aaron
 *
 */
public class ProgressSMsg extends ServiceMessage {
	private static final long serialVersionUID = -5370257906149239288L;
	
	/**
	 * 
	 */
	public final String msg;
	
	/**
	 * 
	 * @param msg
	 */
	public ProgressSMsg(String msg) {
		super(ServiceMessage.ServiceMessageType.PROGRESS);
		this.msg=msg;
	}

}
