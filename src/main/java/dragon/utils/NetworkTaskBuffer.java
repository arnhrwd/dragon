package dragon.utils;

import dragon.tuple.NetworkTask;

public class NetworkTaskBuffer extends CircularBuffer<NetworkTask> {

	public NetworkTaskBuffer() {
		super();
	}
	
	public NetworkTaskBuffer(int bufsize) {
		super(bufsize);
	}
}
