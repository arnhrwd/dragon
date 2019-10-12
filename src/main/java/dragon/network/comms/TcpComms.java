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
import dragon.network.NodeDescriptor;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.service.ServiceDoneSMsg;
import dragon.network.messages.service.ServiceMessage;
import dragon.tuple.NetworkTask;
import dragon.utils.CircularBlockingQueue;

/**
 * An initial implementation of the IComms interface. Based on simple TCP sockets.
 * @author aaron
 *
 */
public class TcpComms implements IComms {
	private static Log log = LogFactory.getLog(TcpComms.class);
	
	private Socket serviceSocketClient;
	private ServerSocket serviceSocketServer;
	private HashMap<String,ObjectOutputStream> serviceOutputStreams;
	private ObjectOutputStream serviceOutputStream;
	private Config conf;

	private LinkedBlockingQueue<ServiceMessage> incomingServiceQueue;
	//LinkedBlockingQueue<ServiceMessage> outgoingServiceQueue;
	private LinkedBlockingQueue<NodeMessage> incomingNodeQueue;
	private CircularBlockingQueue<NetworkTask> incomingTaskQueue;
	private SocketManager socketManager;
	
	private Long id=0L;
	private int resetCount=0;
	private int resetMax=1;
	
	private HashSet<Thread> nodeInputsThreads;
	private HashSet<Thread> taskInputsThreads;
	
	private Thread serviceThread;
	private Thread nodeThread;
	private Thread taskThread;
	
	private NodeDescriptor me;
	
	/**
	 * This method opens comms as a Dragon daemon, using the parameters found in conf
	 * to initialize its own NodeDescriptor.
	 * @param conf
	 * @throws UnknownHostException
	 */
	public TcpComms(Config conf) throws UnknownHostException {
		this.conf=conf;
		incomingServiceQueue = new LinkedBlockingQueue<ServiceMessage>();
		incomingNodeQueue = new LinkedBlockingQueue<NodeMessage>();
		incomingTaskQueue = new CircularBlockingQueue<NetworkTask>(1024);
		nodeInputsThreads = new HashSet<Thread>();
		taskInputsThreads = new HashSet<Thread>();
		resetMax=conf.getDragonCommsResetCount();
	}
	
	/**
	 * Startup only a connection to the provided Dragon node's service port.
	 * Used for communicating service commands only.
	 */
	public void open(NodeDescriptor serviceNode) throws IOException {
		log.debug("opening a service socket to ["+serviceNode+"]");
		serviceSocketClient = new Socket(serviceNode.getHost(),serviceNode.getServicePort());
		serviceOutputStream = new ObjectOutputStream(serviceSocketClient.getOutputStream());
		
		ObjectInputStream in = new ObjectInputStream(serviceSocketClient.getInputStream());
		serviceThread = new Thread() {
			@Override
			public void run() {
				ServiceMessage message;
				try {
					message = (ServiceMessage) in.readObject();
					while(message.getType()!=ServiceMessage.ServiceMessageType.SERVICE_DONE) {
						incomingServiceQueue.put(message);
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
		serviceThread.setName("service");
		serviceThread.start();
	}

	private Long nextId(){
		id++;
		return id;
	}
	
	/**
	 * Open both service and data port server sockets, i.e. to operate as a Dragon daemon.
	 */
	public void open() throws IOException {
		me = conf.getLocalHost();
		log.info("this Dragon node is ["+me+"]");
		serviceSocketServer = new ServerSocket(me.getServicePort());
		socketManager = new SocketManager(me.getDataPort(),me);
		serviceOutputStreams = new HashMap<String,ObjectOutputStream>();
		serviceThread = new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						log.debug("accepting service messages on port ["+serviceSocketServer.getLocalPort()+"]");
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
										incomingServiceQueue.put(message);
										message = (ServiceMessage) in.readObject();
									}
									ServiceDoneSMsg r = new ServiceDoneSMsg();
									r.setMessageId(myid.toString());
									try {
										sendServiceMessage(r);
									} catch (DragonCommsException e) {
										log.error(e.getMessage());
									}
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
						servlet.setName("servlet");
						servlet.start();
						
					} catch (IOException e) {
						log.error("exception with service socket: "+e.toString());
					} 
				}
			}
		};
		serviceThread.setName("service");
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
										incomingNodeQueue.put(message);
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
						t.setName("node input "+nodeInputsThreads.size());
						nodeInputsThreads.add(t);
						t.start();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		nodeThread.setName("node");
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
								ObjectInputStream in = socketManager.getInputStream("task", desc);
								while(!isInterrupted()) {
									try {
										//NetworkTask message = (NetworkTask) socketManager.getInputStream("task", desc).readObject();
										NetworkTask message = (NetworkTask) NetworkTask.readFromStream(in);
										incomingTaskQueue.put(message);
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
						t.setName("task input "+taskInputsThreads.size());
						taskInputsThreads.add(t);
						t.start();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		taskThread.setName("task");
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

	public void sendServiceMessage(ServiceMessage response) throws DragonCommsException {
		// no need to retry sending service message since we cannot form a connection
		// back to the client
		try {
			log.debug("sending service message ["+response.getType().name()+"]");
			if(response.getMessageId().equals("")){
				serviceOutputStream.writeObject(response);
				serviceOutputStream.flush();
			} else {
				synchronized(serviceOutputStreams){
					serviceOutputStreams.get(response.getMessageId()).writeObject(response);
					serviceOutputStreams.get(response.getMessageId()).flush();
					serviceOutputStreams.get(response.getMessageId()).reset();
				}
			}
			return;
		} catch (IOException e) {
				log.error("service data was not transmitted");
		}
		throw new DragonCommsException("service data can not be transmitted");
	}
	
	public void sendServiceMessage(ServiceMessage message, ServiceMessage inResponseTo) throws DragonCommsException {
		message.setMessageId(inResponseTo.getMessageId());
		sendServiceMessage(message);
	}

	public ServiceMessage receiveServiceMessage() throws InterruptedException {
		ServiceMessage m=incomingServiceQueue.take();
		log.debug("received service message ["+m.getType().name()+"]");
		return m;
	}

	public void sendNodeMessage(NodeDescriptor desc, NodeMessage command) throws DragonCommsException {
		command.setSender(me); // node messages typically require to be replied to
		int tries=0;
		while(tries<conf.getDragonCommsRetryAttempts()) {
			try {
				log.debug("sending ["+command.getType().name()+"] to ["+desc+"]");
				socketManager.getOutputStream("node",desc).writeObject(command);
				socketManager.getOutputStream("node",desc).flush();
				socketManager.getOutputStream("node",desc).reset();
				return;
			} catch (IOException e) {
				tries++;
				log.warn("could not connect to ["+desc+
						"]... will retry #["+tries+"] after ["+conf.getDragonCommsRetryMs()+"] ms");
				try {
					Thread.sleep(conf.getDragonCommsRetryMs());
				} catch (InterruptedException e1) {
					log.error("data was not transmitted");
					return;
				}
			}
		}
		log.fatal("data can not be transmitted");
		throw new DragonCommsException("node data can not be transmitted");
	}
	
//	public void sendNodeMessage(NodeDescriptor desc, NodeMessage message) throws DragonCommsException {
//		//message.setMessageId(inResponseTo.getMessageId());
//		sendNodeMessage(desc,message);
//	}

	public NodeMessage receiveNodeMessage() throws InterruptedException {
		return incomingNodeQueue.take();
	}

	public void sendNetworkTask(NodeDescriptor desc, NetworkTask task) throws DragonCommsException {
		int tries=0;
		while(tries<conf.getDragonCommsRetryAttempts()) {
			try {
				synchronized(socketManager.getOutputStream("task", desc)) {
					//socketManager.getOutputStream("task",desc).writeObject(task);
					task.sendToStream(socketManager.getOutputStream("task",desc));
					socketManager.getOutputStream("task", desc).flush();
					resetCount++;
					if(resetCount==resetMax) {
						socketManager.getOutputStream("task",desc).reset();
						resetCount=0;
					}
				}
				return;
			} catch (IOException e) {
				tries++;
				log.warn("could not connect to ["+desc+
						"]... will retry #["+tries+"] after ["+conf.getDragonCommsRetryMs()+"] ms");
				try {
					Thread.sleep(conf.getDragonCommsRetryMs());
				} catch (InterruptedException e1) {
					log.error("data was not transmitted");
					return;
				}
			}
		}
		log.fatal("data can not be transmitted");
		throw new DragonCommsException("task data can not be transmitted");
	}

	public NetworkTask receiveNetworkTask() throws InterruptedException {
		return incomingTaskQueue.take();
	}
}
