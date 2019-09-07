package dragon.network.comms;

import java.io.IOException;

import dragon.NetworkTask;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;

public interface IComms {
	
	/**
	 * Prepare the comms layer for operation. Connect to the supplied node
	 * for service. Implies that other comms features are not required.
	 * @throws IOException 
	 */
	public void open(NodeDescriptor serviceNode) throws IOException;
	
	/**
	 * Prepare the comms layer for operation. Make a service port available
	 * as well as other comms features.
	 * @param serviceOnly
	 * @throws IOException 
	 */
	public void open() throws IOException;
	
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
	 * Locally-blocking send message.
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
