package dragon.utils;

import java.util.HashMap;

/**
 * @author aaron
 *
 */
public class ComponentTaskBuffer extends HashMap<String,StreamTaskBuffer> {
	private static final long serialVersionUID = -1437462550720034299L;
	
	/**
	 * 
	 */
	final int bufSize;
	
	/**
	 * @param bufSize
	 */
	public ComponentTaskBuffer(int bufSize) {
		this.bufSize=bufSize;
	}

	/**
	 * @param destId
	 * @param streamId
	 */
	public void create(String destId, String streamId) {
		if(!containsKey(destId)) {
			put(destId,new StreamTaskBuffer(bufSize));
		}
		get(destId).create(streamId);
	}

}
