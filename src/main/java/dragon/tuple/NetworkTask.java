package dragon.tuple;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * A container class for a tuple, that provides the destination of the
 * tuple in terms of topology, component and tasks that the tuple must be delivered
 * to. This class is recyclable. 
 * @author aaron
 *
 */
public class NetworkTask implements IRecyclable {
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(NetworkTask.class);
	private Tuple tuple;
	private HashSet<Integer> taskIds;
	private String componentId;
	private String topologyId;

	public NetworkTask() {
	
	}
	
	public NetworkTask(Tuple tuple,HashSet<Integer> taskIds,String componentId, String topologyId) {
		init(tuple,taskIds,componentId,topologyId);
	}
	
	public void init(Tuple tuple,HashSet<Integer> taskIds,String componentId, String topologyId) {
		this.tuple=tuple;
		RecycleStation.getInstance().getTupleRecycler(tuple.getFields().getFieldNamesAsString()).shareRecyclable(tuple, 1);
		this.taskIds=taskIds;
		this.componentId=componentId;
		this.topologyId=topologyId;
	}
	
	public Tuple getTuple() {
		return tuple;
	}
	
	public HashSet<Integer> getTaskIds(){
		return taskIds;
	}
	
	public String getComponentId() {
		return componentId;
	}
	
	public String getTopologyId() {
		return topologyId;
	}

	@Override
	public String toString() {
		return tuple.toString();
	}

	@Override
	public void recycle() {
		RecycleStation.getInstance().getTupleRecycler(tuple.getFields().getFieldNamesAsString()).crushRecyclable(tuple, 1);
		tuple=null;
		taskIds=null;
		componentId=null;
		topologyId=null;
	}

	@Override
	public IRecyclable newRecyclable() {
		return new NetworkTask();
	}
	
	public void sendToStream(ObjectOutputStream out) throws IOException {
		tuple.sendToStream(out);
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

	public static NetworkTask readFromStream(ObjectInputStream in) throws ClassNotFoundException, IOException {
		Tuple t = Tuple.readFromStream(in);
		HashSet<Integer> taskIds = new HashSet<Integer>();
		Integer size = in.readInt();
		for(int i=0;i<size;i++) {
			taskIds.add(in.readInt());
		}
		String componentId = (String) in.readUTF();
		String topologyId = (String) in.readUTF();
		NetworkTask nt = RecycleStation.getInstance().getNetworkTaskRecycler().newObject();
		nt.init(t, taskIds, componentId, topologyId);
		RecycleStation.getInstance().getTupleRecycler(t.getFields().getFieldNamesAsString()).crushRecyclable(t, 1);
		return nt;
	}
}