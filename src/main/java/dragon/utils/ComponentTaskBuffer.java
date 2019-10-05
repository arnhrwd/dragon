package dragon.utils;

import java.util.HashMap;

public class ComponentTaskBuffer extends HashMap<String,StreamTaskBuffer> {
	private static final long serialVersionUID = -1437462550720034299L;
	final int bufSize;
	
	public ComponentTaskBuffer(int bufSize) {
		this.bufSize=bufSize;
	}
	

	public void create(String destId, String streamId) {
		if(!containsKey(destId)) {
			put(destId,new StreamTaskBuffer(bufSize));
		}
		get(destId).create(streamId);
	}

}
