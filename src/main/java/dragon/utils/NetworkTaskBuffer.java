package dragon.utils;

import dragon.tuple.NetworkTask;

/**
 * @author aaron
 *
 */
public class NetworkTaskBuffer extends CircularBlockingQueue<NetworkTask> {

	/**
	 * 
	 */
	public NetworkTaskBuffer() {
		super();
	}
	
	/**
	 * @param bufsize
	 */
	public NetworkTaskBuffer(int bufsize) {
		super(bufsize);
	}
}
