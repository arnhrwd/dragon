package dragon.network;

import java.util.HashMap;

import dragon.NetworkTask;
import dragon.utils.CircularBuffer;

public class StreamQueueMap extends HashMap<String,CircularBuffer<NetworkTask>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5785890558744691873L;

	
	public void put(NetworkTask task) throws InterruptedException {
		String streamId = task.getTuple().getSourceStreamId();
		CircularBuffer<NetworkTask> buffer=get(streamId);
		if(buffer==null) {
			buffer=new CircularBuffer<NetworkTask>();
			put(streamId,buffer);
		}
		buffer.put(task);
	}
}
