package dragon.tuple;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RecycleStation {
	private Log log = LogFactory.getLog(RecycleStation.class);
	private static RecycleStation recycleStation=null;
	
	public synchronized static RecycleStation getInstance() {
		if(recycleStation==null) {
			recycleStation=new RecycleStation();
		}
		return recycleStation;
	}
	
	private HashMap<String,Recycler<Tuple>> tupleRecycler;
	private Recycler<NetworkTask> networkTaskRecycler;
	
	public RecycleStation() {
		tupleRecycler=new HashMap<String,Recycler<Tuple>>();
		networkTaskRecycler=new Recycler<NetworkTask>(new NetworkTask(),1024,1024);
	}
	
	public void createTupleRecycler(Tuple tuple,int capacity,int expansion) {
		String id=tuple.getFields().getFieldNamesAsString();
		if(tupleRecycler.containsKey(id)) {
			log.debug("tuple recycler already exists: "+id);
			return;
		}
		tupleRecycler.put(id, new Recycler<Tuple>(tuple,capacity,expansion));
	}
	
	public Recycler<Tuple> getTupleRecycler(String id) {
		return tupleRecycler.get(id);
	}
	
	public Recycler<NetworkTask> getNetworkTaskRecycler(){
		return networkTaskRecycler;
	}
}
