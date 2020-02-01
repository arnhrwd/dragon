package dragon.network;

import java.util.HashMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * A map of node names "hostname:dport" to NodeDescriptors.
 * 
 * @author aaron
 *
 */
public class NodeContext extends HashMap<String,NodeDescriptor>{
	private static final long serialVersionUID = 6956040375029092241L;
	private static final Logger log = LogManager.getLogger(NodeContext.class);
	
	/**
	 * @param desc
	 */
	public synchronized void put(NodeDescriptor desc) {
		put(desc.toString(),desc);
		logContext();
	}
	
	/**
	 * @param desc
	 */
	public synchronized void remove(NodeDescriptor desc) {
		remove(desc.toString());
		logContext();
	}
	
	/**
	 * @param context
	 */
	public synchronized void putAll(NodeContext context) {
		for(String key: context.keySet()) {
			put(key,context.get(key));
		}
		logContext();
	}
	
	/**
	 * 
	 */
	private void logContext() {
		log.debug("context = "+keySet());
	}
}
