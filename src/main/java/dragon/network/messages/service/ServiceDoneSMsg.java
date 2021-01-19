package dragon.network.messages.service;

/**
 * Sent to conclude the service session and thereby close the socket.
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
