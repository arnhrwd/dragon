package dragon.network.messages.service.uploadjar;

import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

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
