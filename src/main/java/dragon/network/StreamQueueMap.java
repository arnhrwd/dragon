package dragon.network;

import java.util.HashMap;

import dragon.NetworkTask;
import dragon.utils.CircularBuffer;

public class StreamQueueMap extends HashMap<String,CircularBuffer<NetworkTask>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5785890558744691873L;

	private int bufferSize;
	
	public StreamQueueMap(int bufferSize){
		this.bufferSize=bufferSize;
	}
	
	public void put(NetworkTask task) throws InterruptedException {
		String streamId = task.getTuple().getSourceStreamId();
		CircularBuffer<NetworkTask> buffer=get(streamId);
		if(buffer==null) {
			buffer=new CircularBuffer<NetworkTask>(bufferSize);
			put(streamId,buffer);
		}
		buffer.put(task);
	}
	
	public CircularBuffer<NetworkTask> getBuffer(NetworkTask task){
		return get(task.getTuple().getSourceStreamId());
	}
}
