package dragon.network.comms;

import java.io.IOException;

import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.tuple.NetworkTask;

public interface IComms {
	
	/**
	 * Prepare the comms layer for operation. Connect to the supplied node
	 * for service. Implies that other comms features are not required.
	 * @param nodeDescriptor The node descriptor to connect to on its service port.
	 * @throws IOException 
	 */
	public void open(NodeDescriptor nodeDescriptor) throws IOException;
	
	/**
	 * Prepare the comms layer for operation. Make a service port available
	 * as well as other comms features, i.e. start up as a Dragon daemon.
	 * @throws IOException 
	 */
	public void open() throws IOException;
	
	/**
	 * Terminate the comms layer operation.
	 */
	public void close();
	
	/**
	 * Always use this method to get the NodeDescriptor for this Dragon daemon.
	 * @return the node descriptor for this node
	 */
	public NodeDescriptor getMyNodeDesc();
	
	/*
	 * Service connections are only initiated by clients. They are not expected to be
	 * high performance, i.e. the implementation does not need to be thread safe.
	 */
	
	/**
	 * Send a service message as the initial message. May block until the message
	 * has been accepted by the OS for transmission, i.e. writing to the output stream.
	 * @param message to send
	 * @throws DragonCommsException if the message could not be sent.
	 */
	public void sendServiceMsg(ServiceMessage message) throws DragonCommsException;
	
	/**
	 * Send a service message in response to a service message. May block until the message
	 * has been accepted by the OS for transmission, i.e. writing to the output stream.
	 * @param message to send
	 * @param inResponseTo message that the message to send is in response to
	 * @throws DragonCommsException if the message could not be sent.
	 */
	public void sendServiceMsg(ServiceMessage message, ServiceMessage inResponseTo) throws DragonCommsException;
	
	/**
	 * Blocking receive message for service messages. A received service message
	 * will have a message id that can be used when responding to it.
	 * @return received message
	 * @throws InterruptedException 
	 */
	public ServiceMessage receiveServiceMsg() throws InterruptedException;
	
	/*
	 * Node connections are initiated only between Dragon daemons. They are not expected to be
	 * high performance, i.e. the implementation does not need to be thread safe.
	 */
	
	public void sendNodeMsg(NodeDescriptor desc, NodeMessage message) throws DragonCommsException;
	
	
	public NodeMessage receiveNodeMsg() throws InterruptedException;
	
	/*
	 * Network task connections are only initiated by clients. They ARE expected to be high
	 * performance and the implementation MUST be thread safe.
	 */
	
	public void sendNetworkTask(NodeDescriptor desc, NetworkTask task) throws DragonCommsException;
	public NetworkTask receiveNetworkTask() throws InterruptedException;
}
