package dragon.utils;

import dragon.tuple.NetworkTask;

public class NetworkTaskBuffer extends CircularBlockingQueue<NetworkTask> {

	public NetworkTaskBuffer() {
		super();
	}
	
	public NetworkTaskBuffer(int bufsize) {
		super(bufsize);
	}
}
