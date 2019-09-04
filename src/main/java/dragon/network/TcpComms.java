package dragon.network;

import dragon.NetworkTask;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;

public class TcpComms implements IComms {

	public void open(boolean serviceOnly) {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void sendServiceMessage(ServiceMessage response) {
		// TODO Auto-generated method stub
		
	}

	public ServiceMessage receiveServiceMessage() {
		// TODO Auto-generated method stub
		return null;
	}

	public void sendNodeMessage(NodeDescriptor desc, NodeMessage command) {
		// TODO Auto-generated method stub
		
	}

	public NodeMessage receiveNodeMessage() {
		// TODO Auto-generated method stub
		return null;
	}

	public void sendNetworkTask(NodeDescriptor desc, NetworkTask task) {
		// TODO Auto-generated method stub
		
	}

	public NetworkTask receiveNetworkTask() {
		// TODO Auto-generated method stub
		return null;
	}

}
