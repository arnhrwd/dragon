package dragon.utils;

import java.util.HashMap;

/**
 * @author aaron
 *
 */
public class StreamTaskBuffer extends HashMap<String,NetworkTaskBuffer> {
	private static final long serialVersionUID = -7939001361124301164L;

	/**
	 * 
	 */
	private int bufSize;
	
	/**
	 * @param bufSize
	 */
	public StreamTaskBuffer(int bufSize) {
		this.bufSize = bufSize;
	}
	
	/**
	 * @param streamId
	 */
	public void create(String streamId) {
		if(!containsKey(streamId)) {
			put(streamId,new NetworkTaskBuffer(bufSize));
		}
		
	}
	

}
