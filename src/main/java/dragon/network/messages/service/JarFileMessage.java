package dragon.network.messages.service;


public class JarFileMessage extends ServiceMessage {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5147221095592953238L;

	public String topologyName;
	public byte[] topologyJar;
	
	public JarFileMessage(String topologyName, byte[] topologyJar) {
		super(ServiceMessage.ServiceMessageType.JARFILE);
		this.topologyName=topologyName;
		this.topologyJar=topologyJar;
	}

}
