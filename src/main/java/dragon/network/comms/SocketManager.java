package dragon.network.comms;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import dragon.network.NodeDescriptor;

public class SocketManager {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1747088785800939467L;

	ServerSocket server;
	TcpStreamMap<ObjectInputStream> inputStreamMap;
	TcpStreamMap<ObjectOutputStream> outputStreamMap;
	TcpStreamMap<Socket> socketMap;
	NodeDescriptor me;
	Thread thread;
	HashMap<String,LinkedBlockingQueue<NodeDescriptor>> inputsWaiting;
	
	public SocketManager(int port,NodeDescriptor me) throws IOException {
		this.me=me;
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
						Socket socket = server.accept();
						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						
						NodeDescriptor endpoint = (NodeDescriptor) in.readObject();
						String id = (String) in.readObject();
						
						synchronized(this) {
							if(!inputStreamMap.contains(id,endpoint)) {
								inputStreamMap.put(id,endpoint,in);
								if(!inputsWaiting.containsKey(id)) {
									inputsWaiting.put(id, new LinkedBlockingQueue<NodeDescriptor>());
								}
								inputsWaiting.get(id).put(endpoint);
							}
							if(!outputStreamMap.contains(id, endpoint)) {
								outputStreamMap.put(id,endpoint,out);
							}
							if(!socketMap.contains(id,endpoint)) {
								socketMap.put(id,endpoint,socket);
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
			if(!inputsWaiting.containsKey(id)) {
				inputsWaiting.put(id, new LinkedBlockingQueue<NodeDescriptor>());
			}
		}
		return inputsWaiting.get(id).take();
	}
	
	public ObjectOutputStream getOutputStream(String id,NodeDescriptor desc) throws IOException {
		synchronized(this) {
			if(outputStreamMap.contains(id,desc)) {
				return outputStreamMap.get(id).get(desc);
			}
		}
		Socket socket = new Socket(desc.host,desc.port);
		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(me);
		out.writeObject(id);
		synchronized(this) {
			if(!inputStreamMap.contains(id,desc)) {
				inputStreamMap.put(id,desc,in);
			}
			if(!outputStreamMap.contains(id, desc)) {
				outputStreamMap.put(id,desc,out);
			}
			if(!socketMap.contains(id,desc)) {
				socketMap.put(id,desc,socket);
			}
			return outputStreamMap.get(id).get(desc);
		}
	}
	
	public ObjectInputStream getInputStream(String id,NodeDescriptor desc) {
		synchronized(this) {
			return inputStreamMap.get(id).get(desc);
		}
	}
}
