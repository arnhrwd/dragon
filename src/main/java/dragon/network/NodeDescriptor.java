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
	
	/**
	 * Used for IP operations.
	 */
	private InetAddress host;
	
	/**
	 * The data port is used to receive stream data.
	 */
	private int dataPort=-1;
	
	/**
	 * The service port is used to receive service commands
	 * and node messages.
	 */
	private int servicePort=-1;
	
	/**
	 * The user provided hostname for this node.
	 */
	private String hostName;
	
	/**
	 * The name that makes this node descriptor unique, which 
	 * is a combination of hostname and data port number.
	 */
	private String fullName;
	
	/**
	 * True if this node is a primary node.
	 */
	private boolean primary;
	
	/**
	 * The partition that this node belongs to.
	 */
	private String partition;
	
	/**
	 * @param hostName
	 * @param dataPort
	 * @param servicePort
	 * @param primary
	 * @param partition
	 * @throws UnknownHostException
	 */
	public NodeDescriptor (String hostName, int dataPort, int servicePort,
			boolean primary, String partition) throws UnknownHostException {
		this.host=InetAddress.getByName(hostName);
		this.dataPort=dataPort;
		this.servicePort=servicePort;
		this.hostName=hostName;
		this.fullName=toString();
		this.primary=primary;
		this.partition=partition;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return hostName+":"+dataPort;
	}
	
	/**
	 * @param host
	 * @throws UnknownHostException
	 */
	public void setHost(String host) throws UnknownHostException {
		this.hostName=host;
		this.fullName=toString();
		this.host=InetAddress.getByName(host);
	}
	
	/**
	 * @return the {@link #hostName}
	 */
	public String getHostName() {
		return hostName;
	}
	
	/**
	 * @param dataPort
	 */
	public void setDataPort(int dataPort) {
		this.dataPort = dataPort;
		this.fullName = toString();
	}
	
	/**
	 * @return
	 */
	public InetAddress getHost() {
		return host;
	}
	
	/**
	 * @return
	 */
	public int getDataPort() {
		if(dataPort==-1) {
			throw new RuntimeException("data port is not set"); //TODO: make these regular exceptions
		}
		return dataPort;
	}
	
	/**
	 * @param servicePort
	 */
	public void setServicePort(int servicePort) {
		this.servicePort=servicePort;
	}
	
	/**
	 * @return
	 */
	public int getServicePort() {
		if(servicePort==-1) {
			throw new RuntimeException("service port is not set"); //TODO: make these regular exceptions
		}
		return servicePort;
	}
	
	/**
	 * @return
	 */
	public boolean isPrimary(){
		return primary;
	}
	
	/**
	 * @return
	 */
	public String getPartition(){
		return partition;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return fullName.hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
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
