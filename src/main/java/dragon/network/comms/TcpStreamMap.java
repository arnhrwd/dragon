package dragon.network.comms;


import java.util.HashMap;

import dragon.network.NodeDescriptor;

/**
 * Maintain a set of TCP streams depending on this destination.
 * @author aaron
 *
 * @param <T> Either an object input stream, output stream or socket.
 */
public class TcpStreamMap<T> extends HashMap<String,HashMap<NodeDescriptor,T>> {
	private static final long serialVersionUID = -646226496373843742L;

	/**
	 * @param id
	 * @param desc
	 * @param obj
	 */
	public void put(String id,NodeDescriptor desc,T obj) {
		if(!containsKey(id)) {
			put(id,new HashMap<NodeDescriptor,T>());
		}
		if(!get(id).containsKey(desc)) {
			get(id).put(desc,obj);
		}
	}
	
	/**
	 * @param id
	 * @param desc
	 * @return
	 */
	public boolean contains(String id,NodeDescriptor desc) {
		if(!containsKey(id)) return false;
		if(!get(id).containsKey(desc)) return false;
		return true;
	}
	
	/**
	 * @param id
	 * @param desc
	 */
	public void drop(String id,NodeDescriptor desc) {
		if(containsKey(id)) {	
			get(id).remove(desc);
			if(get(id).isEmpty()) {
				remove(id);
			}
		}
	}
}
