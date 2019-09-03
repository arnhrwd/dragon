package dragon.network;

import dragon.NetworkTask;

public interface IComms {
	
	public void open();
	public void close();
	
	public void sendServiceResponse(ServiceCommand response);
	public ServiceCommand receiveServiceRequest();
	
	public void sendNodeCommand(NodeDescriptor desc, NodeCommand command);
	public NodeCommand receiveNodeCommand();
	
	public void sendNetworkTask(NodeDescriptor desc, NetworkTask task);
	public NetworkTask receiveNetworkTask();
}
