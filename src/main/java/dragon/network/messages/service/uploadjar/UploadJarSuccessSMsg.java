package dragon.network.messages.service.uploadjar;

import dragon.network.messages.service.ServiceMessage;

/**
 * @author aaron
 *
 */
public class UploadJarSuccessSMsg extends ServiceMessage {
	private static final long serialVersionUID = -6275430939914019067L;
	
	/**
	 * 
	 */
	public String topologyName;
	
	/**
	 * @param topologyName
	 */
	public UploadJarSuccessSMsg(String topologyName) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR_SUCCESS);
		this.topologyName = topologyName;
	}

}
