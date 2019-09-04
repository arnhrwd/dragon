package dragon.network;

import dragon.NetworkTask;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;

public interface IComms {
	
	/**
	 * Prepare the comms layer for operation. Connect to the supplied node
	 * for service. Implies that other comms features are not required.
	 */
	public void open(NodeDescriptor serivceNode);
	
	/**
	 * Prepare the comms layer for operation. Make a service port available
	 * as well as other comms features.
	 * @param serviceOnly
	 */
	public void open();
	
	/**
	 * Terminate the comms layer operation.
	 */
	public void close();
	
	/**
	 * 
	 * @return the node descriptor for this node
	 */
	public NodeDescriptor getMyNodeDescriptor();
	
	
	/**
	 * Non-blocking send message.
	 * @param message to send
	 */
	public void sendServiceMessage(ServiceMessage message);
	
	
	/**
	 * Blocking receive message.
	 * @return received message
	 */
	public ServiceMessage receiveServiceMessage();
	
	
	public void sendNodeMessage(NodeDescriptor desc, NodeMessage message);
	public NodeMessage receiveNodeMessage();
	
	public void sendNetworkTask(NodeDescriptor desc, NetworkTask task);
	public NetworkTask receiveNetworkTask();
}
