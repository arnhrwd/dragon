package dragon.network.comms;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeDescriptor;

public class SocketManager {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1747088785800939467L;
	private static Log log = LogFactory.getLog(SocketManager.class);

	ServerSocket server;
	TcpStreamMap<ObjectInputStream> inputStreamMap;
	TcpStreamMap<ObjectOutputStream> outputStreamMap;
	TcpStreamMap<Socket> socketMap;
	NodeDescriptor me;
	Thread thread;
	HashMap<String,LinkedBlockingQueue<NodeDescriptor>> inputsWaiting;
	SocketManager socketManager;
	
	public SocketManager(int port,NodeDescriptor me) throws IOException {
		this.me=me;
		this.socketManager=this;
		inputStreamMap = new TcpStreamMap<ObjectInputStream>();
		outputStreamMap = new TcpStreamMap<ObjectOutputStream>();
		socketMap = new TcpStreamMap<Socket>();
		inputsWaiting = new HashMap<String,LinkedBlockingQueue<NodeDescriptor>>();
		server = new ServerSocket(port);
		thread=new Thread() {
			@Override
			public void run() {
				while(!isInterrupted()) {
					try {
						log.debug("accepting connections on port ["+server.getLocalPort()+"]");
						Socket socket = server.accept();
						log.debug("new socket from inet address ["+socket.getInetAddress()+"]");
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
						NodeDescriptor endpoint = (NodeDescriptor) in.readObject();
						String id = (String) in.readObject();
						
						log.debug("socket provided handshake ["+endpoint+","+id+"]");
						
						synchronized(socketManager) {
							if(!inputStreamMap.contains(id+"_in",endpoint)) {
								inputStreamMap.put(id+"_in",endpoint,in);
								if(!inputsWaiting.containsKey(id+"_in")) {
									inputsWaiting.put(id+"_in", new LinkedBlockingQueue<NodeDescriptor>());
								}
								inputsWaiting.get(id+"_in").put(endpoint);
							}
							if(!outputStreamMap.contains(id+"_in", endpoint)) {
								outputStreamMap.put(id+"_in",endpoint,out);
							}
							if(!socketMap.contains(id+"_in",endpoint)) {
								socketMap.put(id+"_in",endpoint,socket);
							}
						}
						
					
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		thread.start();
	} 
	
	public NodeDescriptor getWaitingInputs(String id) throws InterruptedException {
		synchronized(this) {
			if(!inputsWaiting.containsKey(id+"_in")) {
				inputsWaiting.put(id+"_in", new LinkedBlockingQueue<NodeDescriptor>());
			}
		}
		return inputsWaiting.get(id+"_in").take();
	}
	
	public ObjectOutputStream getOutputStream(String id,NodeDescriptor desc) throws IOException {
		synchronized(this) {
			if(outputStreamMap.contains(id+"_out",desc)) {
				return outputStreamMap.get(id+"_out").get(desc);
			}
		
			log.debug("creating a socket to ["+desc+"]");
			Socket socket = new Socket(desc.host,desc.port);
			log.debug("writing handshake information ["+me+","+id+"] to ["+desc+"]");
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			out.writeObject(me);
			out.writeObject(id);
			out.flush();
			
			if(!inputStreamMap.contains(id+"_out",desc)) {
				inputStreamMap.put(id+"_out",desc,in);
			}
			if(!outputStreamMap.contains(id+"_out", desc)) {
				outputStreamMap.put(id+"_out",desc,out);
			}
			if(!socketMap.contains(id+"_out",desc)) {
				socketMap.put(id+"_out",desc,socket);
			}
			if(!inputsWaiting.containsKey(id+"_out")) {
				inputsWaiting.put(id+"_out", new LinkedBlockingQueue<NodeDescriptor>());
			}
			try {
				inputsWaiting.get(id+"_out").put(desc);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return outputStreamMap.get(id+"_out").get(desc);
		}
	}
	
	public ObjectInputStream getInputStream(String id,NodeDescriptor desc) {
		synchronized(this) {
			return inputStreamMap.get(id+"_in").get(desc);
		}
	}

	public void delete(String id, NodeDescriptor desc) {
		synchronized(this) {
			inputStreamMap.drop(id,desc);
			outputStreamMap.drop(id,desc);
			socketMap.drop(id,desc);
		}
	}
	
	public void close(String id, NodeDescriptor desc) {
		synchronized(this) {
			try {
				outputStreamMap.get(id).get(desc).close();
				inputStreamMap.get(id).get(desc).close();
				socketMap.get(id).get(desc).close();
			} catch (IOException e) {
				log.error("ioexception while closing a stream: "+e.toString());
			}
		}
		delete(id,desc);
	}
}
