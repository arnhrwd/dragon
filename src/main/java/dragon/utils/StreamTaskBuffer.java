package dragon.utils;

import java.util.HashMap;

public class StreamTaskBuffer extends HashMap<String,NetworkTaskBuffer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7939001361124301164L;

	private int bufSize;
	public StreamTaskBuffer(int bufSize) {
		this.bufSize = bufSize;
	}
	
	public void create(String streamId) {
		if(!containsKey(streamId)) {
			put(streamId,new NetworkTaskBuffer(bufSize));
		}
		
	}
	

}
