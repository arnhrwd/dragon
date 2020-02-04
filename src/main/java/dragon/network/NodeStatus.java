package dragon.network;

import java.io.Serializable;
import java.util.HashMap;

import dragon.LocalCluster;

/**
 * Container class for status information.
 * @author aaron
 *
 */
public class NodeStatus implements Serializable {
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	public long timestamp;
	
	/**
	 * 
	 */
	public NodeDescriptor desc;
	
	/**
	 * 
	 */
	public Node.NodeState state;
	
	/**
	 * 
	 */
	public NodeContext context;
	
	/**
	 * 
	 */
	public HashMap<String,LocalCluster.State> localClusterStates;
	
	/**
	 * 
	 */
	public String partitionId;
	
	/**
	 * 
	 */
	public boolean primary;
	
	/**
	 * 
	 */
	public NodeStatus() {
		localClusterStates=new HashMap<String,LocalCluster.State>();
	}
}
