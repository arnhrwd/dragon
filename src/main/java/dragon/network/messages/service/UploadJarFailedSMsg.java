package dragon.network.messages.service;

/**
 * @author aaron
 *
 */
public class UploadJarFailedSMsg extends ServiceErrorMessage {
	private static final long serialVersionUID = -562124896589218017L;
	
	/**
	 * 
	 */
	public final String topologyId;
	
	/**
	 * @param topologyId
	 * @param error
	 */
	public UploadJarFailedSMsg(String topologyId,String error) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR_FAILED,error);
		this.topologyId=topologyId;
	}
	
}
