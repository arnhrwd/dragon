package dragon.network;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A map of node names "hostname:dport" to NodeDescriptors.
 * 
 * @author aaron
 *
 */
public class NodeContext extends HashMap<String,NodeDescriptor>{
	private static final long serialVersionUID = 6956040375029092241L;
	private static final Log log = LogFactory.getLog(Node.class);
	
	public synchronized void put(NodeDescriptor desc) {
		put(desc.toString(),desc);
		logContext();
	}
	
	public synchronized void remove(NodeDescriptor desc) {
		remove(desc.toString());
		logContext();
	}
	
	public synchronized void putAll(NodeContext context) {
		for(String key: context.keySet()) {
			put(key,context.get(key));
		}
		logContext();
	}
	
	private void logContext() {
		log.debug("context = "+keySet());
	}
}
