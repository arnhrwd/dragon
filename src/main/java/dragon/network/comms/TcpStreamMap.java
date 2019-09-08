package dragon.network.comms;


import java.util.HashMap;

import dragon.network.NodeDescriptor;

public class TcpStreamMap<T> extends HashMap<String,HashMap<NodeDescriptor,T>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -646226496373843742L;

	public void put(String id,NodeDescriptor desc,T obj) {
		if(!containsKey(id)) {
			put(id,new HashMap<NodeDescriptor,T>());
		}
		if(!get(id).containsKey(desc)) {
			get(id).put(desc,obj);
		}
	}
	
	public boolean contains(String id,NodeDescriptor desc) {
		if(!containsKey(id)) return false;
		if(!get(id).containsKey(desc)) return false;
		return true;
	}
	
	public void drop(String id,NodeDescriptor desc) {
		remove(id);
	}
}
