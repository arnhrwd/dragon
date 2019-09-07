package dragon.network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.NetworkTask;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;


public class TcpComms implements IComms {
	private static Log log = LogFactory.getLog(TcpComms.class);
	
	Socket serviceSocketClient;
	ServerSocket serviceSocketServer;

	LinkedBlockingQueue<ServiceMessage> incommingServiceQueue;
	LinkedBlockingQueue<ServiceMessage> outgoingServiceQueue;
	
	Thread serviceThread;
	
	public TcpComms(Config conf) {
		incommingServiceQueue = new LinkedBlockingQueue<ServiceMessage>();
		outgoingServiceQueue = new LinkedBlockingQueue<ServiceMessage>();
	}
	
	public void open(NodeDescriptor serviceNode) throws IOException {
		serviceSocketClient = new Socket(serviceNode.host,serviceNode.port);
	}

	
	public void open() throws IOException {
		serviceSocketServer = new ServerSocket(4000);
		serviceThread = new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						Socket socket = serviceSocketServer.accept();
						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
						ServiceMessage message = (ServiceMessage) in.readObject();
						while(message.getType()!=ServiceMessage.ServiceMessageType.SERVICE_DONE) {
							incommingServiceQueue.put(message);
							message = (ServiceMessage) in.readObject();
						}
						in.close();
						socket.close();
					} catch (IOException e) {
						log.error("exception with service socket: "+e.toString());
						interrupt();
					} catch (ClassNotFoundException e) {
						log.error("something other than a ServiceMessage was received: "+e.toString());
						interrupt();
					} catch (InterruptedException e) {
						log.error("interrupted while putting on incomming service queue: "+e.toString());
						interrupt();
					}
				}
			}
		};
		serviceThread.start();
	}

	public void close() {
		serviceThread.interrupt();
		
	}
	
	public NodeDescriptor getMyNodeDescriptor() {
		return null;
		
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
