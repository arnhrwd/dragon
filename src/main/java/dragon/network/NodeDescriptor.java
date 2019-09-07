package dragon.network;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NodeDescriptor implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7410142802709962977L;
	public InetAddress host;
	public int port;
	private String hostName;
	private String fullName;
	
	public NodeDescriptor (String hostName,int port) throws UnknownHostException {
		this.host=InetAddress.getByName(hostName);
		this.port=port;
		this.hostName=hostName;
		this.fullName=toString();
	}
	
	public String toString() {
		return hostName+":"+port;
	}
	
	public void setHost(String host) throws UnknownHostException {
		this.host=InetAddress.getByName(host);
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	@Override
	public int hashCode() {
		return fullName.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final NodeDescriptor other = (NodeDescriptor) obj;
        if (fullName == null) {
            if (other.fullName != null)
                return false;
        } else if (!fullName.equals(other.fullName))
            return false;
        return true;
	}
}
