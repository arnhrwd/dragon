package dragon.network.messages.service.uploadjar;

import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;

/**
 * @author aaron
 *
 */
public class UploadJarSMsg extends ServiceMessage {
	private static final long serialVersionUID = 5147221095592953238L;

	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * 
	 */
	public byte[] topologyJar;
	
	/**
	 * @param topologyName
	 * @param topologyJar
	 */
	public UploadJarSMsg(String topologyName, byte[] topologyJar) {
		super(ServiceMessage.ServiceMessageType.UPLOAD_JAR);
		this.topologyId=topologyName;
		this.topologyJar=topologyJar;
	}

}
