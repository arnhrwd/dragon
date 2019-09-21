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
import dragon.network.messages.service.ServiceDoneMessage;
import dragon.network.messages.service.ServiceMessage;


public class TcpComms implements IComms {
	private static Log log = LogFactory.getLog(TcpComms.class);
	
	private Socket serviceSocketClient;
	private ServerSocket serviceSocketServer;
	private HashMap<String,ObjectOutputStream> serviceOutputStreams;
	private ObjectOutputStream serviceOutputStream;
	private Config conf;

	private LinkedBlockingQueue<ServiceMessage> incommingServiceQueue;
	//LinkedBlockingQueue<ServiceMessage> outgoingServiceQueue;
	private LinkedBlockingQueue<NodeMessage> incommingNodeQueue;
	private LinkedBlockingQueue<NetworkTask> incommingTaskQueue;
	private SocketManager socketManager;
	
	private Long id=0L;
	
	private HashSet<Thread> nodeInputsThreads;
	private HashSet<Thread> taskInputsThreads;
	
	private Thread serviceThread;
	private Thread nodeThread;
	private Thread taskThread;
	
	private NodeDescriptor me;
	
	public TcpComms(Config conf) throws UnknownHostException {
		this.conf=conf;
		me = new NodeDescriptor(conf.getDragonNetworkLocalHost(),
			conf.getDragonNetworkLocalNodePort());
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
					serviceOutputStream.close();
					serviceSocketClient.close();
				} catch (ClassNotFoundException | IOException e2) {
					log.debug("class not found or ioexception: "+e2.toString());
				} catch (InterruptedException e) {
					log.debug("interrupted");
				}
				log.debug("service done");
			}
		};
		serviceThread.start();
	}

	private Long nextId(){
		id++;
		return id;
	}
	
	public void open() throws IOException {
		serviceSocketServer = new ServerSocket(conf.getDragonNetworkLocalServicePort());
		socketManager = new SocketManager(conf.getDragonNetworkLocalNodePort(),me);
		serviceOutputStreams = new HashMap<String,ObjectOutputStream>();
		serviceThread = new Thread() {
			@Override
			public void run() {
				
				while(!isInterrupted()) {
					try {
						log.debug("accepting service messages");
						Socket socket = serviceSocketServer.accept();
						id=id+1;
						Thread servlet = new Thread(){
							Long myid = nextId();
							@Override
							public void run(){
								try  {
									synchronized(serviceOutputStreams){
										serviceOutputStreams.put(myid.toString(), new ObjectOutputStream(socket.getOutputStream()));
									}
									ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
									ServiceMessage message = (ServiceMessage) in.readObject();
									while(message.getType()!=ServiceMessage.ServiceMessageType.SERVICE_DONE) {
										message.setMessageId(myid.toString());
										incommingServiceQueue.put(message);
										message = (ServiceMessage) in.readObject();
									}
									ServiceDoneMessage r = new ServiceDoneMessage();
									r.setMessageId(myid.toString());
									sendServiceMessage(r);
									synchronized(serviceOutputStreams){
										serviceOutputStreams.get(myid.toString()).close();
										serviceOutputStreams.remove(myid.toString());
									}
									in.close();
									socket.close();
								} catch (IOException e){
									log.error("exception with service socket: "+e.toString());
								} catch (ClassNotFoundException e) {
									log.error("something other than a ServiceMessage was received: "+e.toString());
								} catch (InterruptedException e) {
									log.warn("interrupted while putting on incomming service queue: "+e.toString());
								}
							}
						};
						servlet.start();
						
					} catch (IOException e) {
						log.error("exception with service socket: "+e.toString());
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
										log.error("ioexception on node stream from ["+desc+"]: "+e.toString());
										socketManager.delete("node",desc);
										break;
									} catch (ClassNotFoundException e) {
										log.error("incorrect class transmitted on node stream from +["+desc+"]");
										socketManager.close("node",desc);
										break;
									} catch (InterruptedException e) {
										log.warn("interrupted while reading node stream from ["+desc+"]");
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
			if(response.getMessageId().equals("")){
				serviceOutputStream.writeObject(response);
				serviceOutputStream.flush();
			} else {
				synchronized(serviceOutputStreams){
					serviceOutputStreams.get(response.getMessageId()).writeObject(response);
					serviceOutputStreams.get(response.getMessageId()).flush();
				}
			}
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
			//log.debug("sending ["+task+"] to ["+desc+"]");
			synchronized(socketManager.getOutputStream("task", desc)) {
				socketManager.getOutputStream("task",desc).writeObject(task);
				socketManager.getOutputStream("task", desc).flush();
			}
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
