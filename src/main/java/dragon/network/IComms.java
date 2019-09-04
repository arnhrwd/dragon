package dragon.network;

import dragon.NetworkTask;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;

public interface IComms {
	
	/**
	 * Prepare the comms layer for operation.
	 * @param serviceOnly is true if only the service features are required.
	 */
	public void open(boolean serviceOnly);
	
	/**
	 * Terminate the comms layer operation.
	 */
	public void close();
	
	public NodeDescriptor createNodeDescriptor(String hostname,int port);
	
	
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
