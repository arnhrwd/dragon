package dragon.network.messages.node.preparejar;

import dragon.network.Node;
import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class PrepareJarNMsg extends NodeMessage {
	private static final long serialVersionUID = -7900745704249481502L;
	
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
	public PrepareJarNMsg(String topologyName, byte[] topologyJar) {
		super(NodeMessage.NodeMessageType.PREPARE_JAR);
		this.topologyId=topologyName;
		this.topologyJar=topologyJar;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		final Node node  = Node.inst();
		if(!node.storeJarFile(topologyId,topologyJar)) {
			sendError("could not store the topology jar");
			return;
		} else if(!node.loadJarFile(topologyId)) {
			sendError("could not load the topology jar");
			return;
		}
		sendSuccess();
	}

}
