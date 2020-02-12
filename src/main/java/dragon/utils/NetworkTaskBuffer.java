package dragon.utils;

import java.util.concurrent.locks.ReentrantLock;

import org.jctools.queues.SpscArrayQueue;

import dragon.tuple.NetworkTask;

/**
 * @author aaron
 *
 */
public class NetworkTaskBuffer extends SpscArrayQueue<NetworkTask> {
	public final ReentrantLock bufferLock = new ReentrantLock();
	/**
	 * 
	 */
	//public NetworkTaskBuffer() {
		//super();
	//}
	
	/**
	 * @param bufsize
	 */
	public NetworkTaskBuffer(int bufsize) {
		super(bufsize);
	}
}
