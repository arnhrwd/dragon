package dragon.network.messages.service;

public class UploadJarSuccessMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6275430939914019067L;
	public String topologyName;
	public UploadJarSuccessMessage(String topologyName) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR_SUCCESS);
		this.topologyName = topologyName;
	}

}
