package dragon.network.messages.service;

/**
 * @author aaron
 *
 */
public class ServiceDoneSMsg extends ServiceMessage {
	private static final long serialVersionUID = 2960892077094719654L;

	/**
	 * 
	 */
	public ServiceDoneSMsg() {
		super(ServiceMessageType.SERVICE_DONE);
	}
	
}
