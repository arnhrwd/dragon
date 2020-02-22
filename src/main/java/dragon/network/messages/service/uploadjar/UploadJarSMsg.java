package dragon.network.messages.service.uploadjar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.network.Node;
import dragon.network.messages.service.ServiceMessage;

/**
 * Upload a JAR file to the daemon. JAR files that contain the topology must be
 * uploaded prior to the run topology commmand, on the same daemon that the run
 * topology command will be given to.
 * 
 * @author aaron
 *
 */
public class UploadJarSMsg extends ServiceMessage {
	private static final long serialVersionUID = 5147221095592953238L;
	private final static Logger log = LogManager.getLogger(UploadJarSMsg.class);

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
	
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node=Node.inst();
		if (node.getLocalClusters().containsKey(topologyId)) {
			client(new UploadJarFailedSMsg(topologyId, "topology exists"));
		} else {
			log.info("storing topology [" + topologyId + "]");
			if (!node.storeJarFile(topologyId, topologyJar)) {
				client(new UploadJarFailedSMsg(topologyId, "could not store the topology jar"));
				return;
			}
			if (!node.loadJarFile(topologyId)) {
				client(new UploadJarFailedSMsg(topologyId, "could not load the topology jar"));
				return;
			}
			client(new UploadJarSuccessSMsg(topologyId));
		}
	}

}
