package dragon.network.comms;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
		log.debug("this node is ["+me+"]");
		incommingServiceQueue = new LinkedBlockingQueue<ServiceMessage>();
		incommingNodeQueue = new LinkedBlockingQueue<NodeMessage>();
		incommingTaskQueue = new LinkedBlockingQueue<NetworkTask>();
		nodeInputsThreads = new HashSet<Thread>();
		taskInputsThreads = new HashSet<Thread>();
	}
	
	public void open(NodeDescriptor serviceNode) throws IOException {
		log.debug("opening a service socket to ["+serviceNode+"]");
		serviceSocketClient = new Socket(serviceNode.host,serviceNode.port);
		serviceOutputStream = new ObjectOutputStream(serviceSocketClient.getOutputStream());
		ObjectInputStream in = new ObjectInputStream(serviceSocketClient.getInputStream());
		serviceThread = new Thread() {
			@Override
			public void run() {
				ServiceMessage message;
				try {
					message = (ServiceMessage) in.readObject();
					while(message.getType()!=ServiceMessage.ServiceMessageType.SERVICE_DONE) {
						incommingServiceQueue.put(message);
						message = (ServiceMessage) in.readObject();
					}
					in.close();
					serviceSocketClient.close();
				} catch (ClassNotFoundException | IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		serviceThread.start();
	}

	
	public void open() throws IOException {
		serviceSocketServer = new ServerSocket((Integer)conf.get(Config.DRAGON_NETWORK_LOCAL_SERVICE_PORT));
		socketManager = new SocketManager((Integer)conf.get(Config.DRAGON_NETWORK_LOCAL_NODE_PORT),me);
		
		serviceThread = new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						log.debug("accepting service messages");
						Socket socket = serviceSocketServer.accept();
						serviceOutputStream = new ObjectOutputStream(socket.getOutputStream());
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
					} catch (ClassNotFoundException e) {
						log.error("something other than a ServiceMessage was received: "+e.toString());
					} catch (InterruptedException e) {
						log.warn("interrupted while putting on incomming service queue: "+e.toString());
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
									} catch (IOException e) {
										log.error("ioexception on node stream from +["+desc+"]: "+e.toString());
										socketManager.delete("node",desc);
										break;
									} catch (ClassNotFoundException e) {
										log.error("incorrect class transmitted on node stream from +["+desc+"]");
										socketManager.close("node",desc);
										break;
									} catch (InterruptedException e) {
										log.warn("interrupted while reading node stream from +["+desc+"]");
										socketManager.close("node",desc);
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
									} catch (IOException e) {
										log.error("ioexception on task stream from +["+desc+"]: "+e.toString());
										socketManager.delete("task",desc);
										break;
									} catch (ClassNotFoundException e) {
										log.error("incorrect class transmitted on task stream from +["+desc+"]");
										socketManager.close("task",desc);
										break;
									} catch (InterruptedException e) {
										log.warn("interrupted while reading node stream from +["+desc+"]");
										socketManager.close("node",desc);
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
			log.debug("sending service message ["+response.getType().name()+"]");
			serviceOutputStream.writeObject(response);
			serviceOutputStream.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public ServiceMessage receiveServiceMessage() {
		ServiceMessage m=null;
		try {
			m=incommingServiceQueue.take();
			log.debug("received service message ["+m.getType().name()+"]");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return m;
	}

	public void sendNodeMessage(NodeDescriptor desc, NodeMessage command) {
		command.setSender(me);
		try {
			log.debug("sending ["+command.getType().name()+"] to ["+desc+"]");
			socketManager.getOutputStream("node",desc).writeObject(command);
			socketManager.getOutputStream("node",desc).flush();
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
			log.debug("sending ["+task+"] to ["+desc+"]");
			socketManager.getOutputStream("task",desc).writeObject(task);
			socketManager.getOutputStream("task", desc).flush();
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
