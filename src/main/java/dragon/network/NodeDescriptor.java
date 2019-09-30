package dragon.network;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The NodeDescriptor provides information that is required to identify a node,
 * including the data and service ports it is listening on, and its advertised
 * host name. The NodeDescriptor gives a unique identification of a Dragon node
 * based on the combination of advertised host name and data port.
 * @author aaron
 *
 */
public class NodeDescriptor implements Serializable {
	private static final long serialVersionUID = -7410142802709962977L;
	private InetAddress host;
	private int dataPort=-1;
	private int servicePort=-1;
	private String hostName;
	private String fullName;
	
	public NodeDescriptor (String hostName, int dataPort, int servicePort) throws UnknownHostException {
		this.host=InetAddress.getByName(hostName);
		this.dataPort=dataPort;
		this.servicePort=servicePort;
		this.hostName=hostName;
		this.fullName=toString();
	}
	
	public String toString() {
		return hostName+":"+dataPort;
	}
	
	public void setHost(String host) throws UnknownHostException {
		this.hostName=host;
		this.fullName=toString();
		this.host=InetAddress.getByName(host);
	}
	
	public void setDataPort(int dataPort) {
		this.dataPort = dataPort;
		this.fullName = toString();
	}
	
	public InetAddress getHost() {
		return host;
	}
	
	public int getDataPort() {
		if(dataPort==-1) {
			throw new RuntimeException("data port is not set");
		}
		return dataPort;
	}
	
	public void setServicePort(int servicePort) {
		this.servicePort=servicePort;
	}
	
	public int getServicePort() {
		if(servicePort==-1) {
			throw new RuntimeException("service port is not set");
		}
		return servicePort;
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
