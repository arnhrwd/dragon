package dragon.network.messages.service;

public class UploadJarFailedMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -562124896589218017L;
	public String topologyId;
	public String error;
	public UploadJarFailedMessage(String topologyId,String error) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR_FAILED);
		this.topologyId=topologyId;
		this.error=error;
	}

}
