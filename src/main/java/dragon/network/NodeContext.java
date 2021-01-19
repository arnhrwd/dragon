package dragon.network;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A map of node names "hostname:dport" to NodeDescriptors.
 * For convenience elsewhere the hashcode of a NodeDescriptor is the
 * hashcode of the string "hostname:dport" and toString() returns
 * the string "hostname:dport". In this class we explicitly use the
 * toString() value for the key. In some cases, just getting the
 * keySet() of this object, which is a set of names, is desired.
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
		context.forEach((k,v)->{
			put(k,v);
		});
		logContext();
	}
	
	/**
	 * 
	 */
	private void logContext() {
		log.debug("context = "+keySet());
	}
}
