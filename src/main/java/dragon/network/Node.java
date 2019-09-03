package dragon.network;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dragon.Config;

public class Node {
	private IComms comms;
	private ExecutorService networkExecutorService;
	public Node(Config conf) {
		comms = new TcpComms();
		
	}
	
	
}
