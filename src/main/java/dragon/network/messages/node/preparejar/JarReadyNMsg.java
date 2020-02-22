package dragon.network.messages.node.preparejar;

import dragon.network.messages.node.NodeMessage;

/**
 * @author aaron
 *
 */
public class JarReadyNMsg extends NodeMessage {
	private static final long serialVersionUID = 8486176138295531405L;
	
	/**
	 * 
	 */
	public String topologyId;
	
	/**
	 * @param topologyId
	 */
	public JarReadyNMsg(String topologyId) {
		super(NodeMessage.NodeMessageType.JAR_READY);
		this.topologyId = topologyId;
	}
	
	/**
	 *
	 */
	@Override
	public void process() {
		receiveSuccess();
	}

}
