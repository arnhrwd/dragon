package dragon.network;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.tuple.NetworkTask;
import dragon.utils.NetworkTaskBuffer;

/**
 * Store queues for streams.
 * @author aaron
 *
 */
public class StreamQueueMap extends HashMap<String,NetworkTaskBuffer>{
	private static Logger log = LogManager.getLogger(StreamQueueMap.class);
	private static final long serialVersionUID = 5785890558744691873L;
	
	/**
	 * 
	 */
	private final int bufferSize;
	
	/**
	 * @param bufferSize
	 */
	public StreamQueueMap(int bufferSize){
		this.bufferSize=bufferSize;
	}
	
	/**
	 * @param task
	 * @throws InterruptedException
	 */
	public void put(NetworkTask task) throws InterruptedException {
		String streamId = task.getTuple().getSourceStreamId();
		NetworkTaskBuffer buffer=get(streamId);
		buffer.put(task);
	}
	
	/**
	 * @param task
	 * @return
	 */
	public NetworkTaskBuffer getBuffer(NetworkTask task){
		return get(task.getTuple().getSourceStreamId());
	}

	/**
	 * @param streamId
	 */
	public void prepare(String streamId) {
		if(!containsKey(streamId)) {
			NetworkTaskBuffer buffer=new NetworkTaskBuffer(bufferSize);
			put(streamId,buffer);
		}
	}

	/**
	 * @param streamId
	 */
	public void drop(String streamId) {
		if(containsKey(streamId)) {
			if(get(streamId).size()!=0) {
				log.error("dropping stream ["+streamId+"] of size ["+get(streamId).size()+"]");
			}
			remove(streamId);
		}
	}
}
