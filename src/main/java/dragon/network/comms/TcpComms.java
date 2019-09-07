package dragon.network.comms;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.NetworkTask;
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceMessage;


public class TcpComms implements IComms {
	private static Log log = LogFactory.getLog(TcpComms.class);
	
	Socket serviceSocketClient;
	ServerSocket serviceSocketServer;
	ObjectOutputStream serviceOutputStream;
	Config conf;

	LinkedBlockingQueue<ServiceMessage> incommingServiceQueue;
	//LinkedBlockingQueue<ServiceMessage> outgoingServiceQueue;
	LinkedBlockingQueue<NodeMessage> incommingNodeQueue;
	LinkedBlockingQueue<NetworkTask> incommingTaskQueue;
	SocketManager socketManager;
	
	
	
	HashSet<Thread> nodeInputsThreads;
	HashSet<Thread> taskInputsThreads;
	
	Thread serviceThread;
	Thread nodeThread;
	Thread taskThread;
	
	NodeDescriptor me;
	
	public TcpComms(Config conf) throws UnknownHostException {
		this.conf=conf;
		me = new NodeDescriptor((String)conf.get(Config.DRAGON_NETWORK_LOCAL_HOST),
				(Integer)conf.get(Config.DRAGON_NETWORK_LOCAL_NODE_PORT));
		incommingServiceQueue = new LinkedBlockingQueue<ServiceMessage>();
		incommingNodeQueue = new LinkedBlockingQueue<NodeMessage>();
		incommingTaskQueue = new LinkedBlockingQueue<NetworkTask>();
		nodeInputsThreads = new HashSet<Thread>();
		taskInputsThreads = new HashSet<Thread>();
	}
	
	public void open(NodeDescriptor serviceNode) throws IOException {
		serviceSocketClient = new Socket(serviceNode.host,serviceNode.port);
		serviceOutputStream = new ObjectOutputStream(serviceSocketClient.getOutputStream());
	}

	
	public void open() throws IOException {
		serviceSocketServer = new ServerSocket((Integer)conf.get(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT));
		socketManager = new SocketManager((Integer)conf.get(Config.DRAGON_NETWORK_LOCAL_NODE_PORT),me);
		
		serviceThread = new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						Socket socket = serviceSocketServer.accept();
						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
						serviceOutputStream = new ObjectOutputStream(socket.getOutputStream());
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
		
		nodeThread = new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						NodeDescriptor desc = socketManager.getWaitingInputs("node");
						Thread t = new Thread() {
							@Override
							public void run() {
								while(!isInterrupted()) {
									try {
										NodeMessage message = (NodeMessage) socketManager.getInputStream("node", desc).readObject();
										incommingNodeQueue.put(message);
									} catch (ClassNotFoundException | IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}
						};
						nodeInputsThreads.add(t);
						t.start();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		nodeThread.start();
		
		taskThread = new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						NodeDescriptor desc = socketManager.getWaitingInputs("task");
						Thread t = new Thread() {
							@Override
							public void run() {
								while(!isInterrupted()) {
									try {
										NetworkTask message = (NetworkTask) socketManager.getInputStream("task", desc).readObject();
										incommingTaskQueue.put(message);
									} catch (ClassNotFoundException | IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}
						};
						taskInputsThreads.add(t);
						t.start();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		taskThread.start();
		
		
	}

	public void close() {
		serviceThread.interrupt();
		try {
			serviceOutputStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(serviceSocketServer!=null) {
			try {
				serviceSocketServer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public NodeDescriptor getMyNodeDescriptor() {
		return me;
		
	}

	public void sendServiceMessage(ServiceMessage response) {
		try {
			serviceOutputStream.writeObject(response);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public ServiceMessage receiveServiceMessage() {
		ServiceMessage m=null;
		try {
			m=incommingServiceQueue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return m;
	}

	public void sendNodeMessage(NodeDescriptor desc, NodeMessage command) {
		try {
			socketManager.getOutputStream("node",desc).writeObject(command);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public NodeMessage receiveNodeMessage() {
		NodeMessage m=null;
		try {
			m=incommingNodeQueue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return m;
	}

	public void sendNetworkTask(NodeDescriptor desc, NetworkTask task) {
		try {
			socketManager.getOutputStream("task",desc).writeObject(task);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public NetworkTask receiveNetworkTask() {
		NetworkTask m=null;
		try {
			m=incommingTaskQueue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return m;
	}
}
