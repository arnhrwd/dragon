package dragon.network;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NodeDescriptor {
	public InetAddress host;
	public int port;
	
	public NodeDescriptor (String host,int port) throws UnknownHostException {
		this.host=InetAddress.getByName(host);
		this.port=port;
	}
	
	public String toString() {
		return host.getHostAddress()+":"+port;
	}
	
	public void setHost(String host) throws UnknownHostException {
		this.host=InetAddress.getByName(host);
	}
	
	public void setPort(int port) {
		this.port = port;
	}
}
