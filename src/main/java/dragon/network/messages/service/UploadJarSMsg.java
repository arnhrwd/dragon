package dragon.network.messages.service;


public class UploadJarSMsg extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5147221095592953238L;

	public String topologyName;
	public byte[] topologyJar;
	
	public UploadJarSMsg(String topologyName, byte[] topologyJar) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR);
		this.topologyName=topologyName;
		this.topologyJar=topologyJar;
	}

}
