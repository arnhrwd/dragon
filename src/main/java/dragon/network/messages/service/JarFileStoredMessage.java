package dragon.network.messages.service;

public class JarFileStoredMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6275430939914019067L;
	public String topologyName;
	public JarFileStoredMessage(String topologyName) {
		super(ServiceMessage.ServiceMessageType.JARFILE_STORED);
		this.topologyName = topologyName;
	}

}
