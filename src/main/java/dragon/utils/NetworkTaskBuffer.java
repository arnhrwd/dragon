package dragon.utils;

import dragon.NetworkTask;

public class NetworkTaskBuffer extends CircularBuffer<NetworkTask> {

	public NetworkTaskBuffer() {
		super();
	}
	
	public NetworkTaskBuffer(int bufsize) {
		super(bufsize);
	}
}
