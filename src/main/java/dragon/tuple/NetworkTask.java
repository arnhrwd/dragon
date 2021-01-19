package dragon.tuple;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A container class for a tuple, that provides the destination of the
 * tuple in terms of topology, component and tasks that the tuple must be delivered
 * to. This class is recyclable. 
 * @author aaron
 *
 */
public class NetworkTask {
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(NetworkTask.class);
	
	/**
	 * All tuples MUST have the same source component, task and stream ids.
	 * They should be processed in the order of the array. Processing can
	 * stop as soon as the first null element is reached.
	 */
	private Tuple[] tuples;
	
	/*
	 * Destination of the tuples is below.
	 */
	
	/**
	 * 
	 */
	private HashSet<Integer> taskIds;
	
	/**
	 * 
	 */
	private String componentId;
	
	/**
	 * 
	 */
	private String topologyId;

	/**
	 * 
	 */
	public NetworkTask() {
	
	}
	
	/**
	 * @param tuples
	 * @param taskIds
	 * @param componentId
	 * @param topologyId
	 */
	public NetworkTask(Tuple[] tuples,HashSet<Integer> taskIds,String componentId, String topologyId) {
		init(tuples,taskIds,componentId,topologyId);
	}
	
	/**
	 * @param tuples
	 * @param taskIds
	 * @param componentId
	 * @param topologyId
	 */
	public void init(Tuple[] tuples,HashSet<Integer> taskIds,String componentId, String topologyId) {
		this.tuples=tuples;
		this.taskIds=taskIds;
		this.componentId=componentId;
		this.topologyId=topologyId;
	}
	
	/**
	 * @return
	 */
	public Tuple[] getTuples() {
		return tuples;
	}
	
	/**
	 * @return
	 */
	public HashSet<Integer> getTaskIds(){
		return taskIds;
	}
	
	/**
	 * @return
	 */
	public String getComponentId() {
		return componentId;
	}
	
	/**
	 * @return
	 */
	public String getTopologyId() {
		return topologyId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return tuples.toString();
	}
	
	/**
	 * @param out
	 * @throws IOException
	 */
	public void sendToStream(ObjectOutputStream out) throws IOException {
		int size=0;
		for(;size<tuples.length&&tuples[size]!=null;size++);
		out.writeInt(size);
		for(int i=0;i<size;i++) {
			tuples[i].sendToStream(out);
		}
//		out.writeObject(taskIds);
//		out.writeObject(componentId);
//		out.writeObject(topologyId);
		out.writeInt(taskIds.size());
		for(Integer taskId : taskIds) {
			out.writeInt(taskId);
		}
		out.writeUTF(componentId);
		out.writeUTF(topologyId);
	}

	/**
	 * @param in
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public static NetworkTask readFromStream(ObjectInputStream in) throws ClassNotFoundException, IOException {
		Integer size = in.readInt();
		Tuple[] tuples=new Tuple[size];
		for(int i=0;i<size;i++) {
			tuples[i] = Tuple.readFromStream(in);
		}
		HashSet<Integer> taskIds = new HashSet<Integer>();
		Integer num = in.readInt();
		for(int i=0;i<num;i++) {
			taskIds.add(in.readInt());
		}
		String componentId = (String) in.readUTF();
		String topologyId = (String) in.readUTF();
		NetworkTask nt = new NetworkTask();
		nt.init(tuples, taskIds, componentId, topologyId);
		return nt;
	}
}