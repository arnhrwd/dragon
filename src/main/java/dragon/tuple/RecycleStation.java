package dragon.tuple;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;

public class RecycleStation {
	private Log log = LogFactory.getLog(RecycleStation.class);
	private static RecycleStation recycleStation=null;
	private Config conf;
	
	public static void instanceInit(Config conf) {
		recycleStation=new RecycleStation(conf);
	}
	
	public static RecycleStation getInstance() {
		return recycleStation;
	}
	
	private HashMap<String,Recycler<Tuple>> tupleRecycler;
	private Recycler<NetworkTask> networkTaskRecycler;
	//private Recycler<RTaskSet> taskSetRecycler;
	
	public RecycleStation(Config conf) {
		this.conf=conf;
		tupleRecycler=new HashMap<String,Recycler<Tuple>>();
		networkTaskRecycler=new Recycler<NetworkTask>(new NetworkTask(),
				conf.getDragonRecyclerTaskCapacity(),
				conf.getDragonRecyclerTaskExpansion(),
				conf.getDragonRecyclerTaskCompact());
		//taskSetRecycler=new Recycler<RTaskSet>(new RTaskSet(),1024,1024);
	}
	
	public void createTupleRecycler(Tuple tuple) {
		String id=tuple.getFields().getFieldNamesAsString();
		if(tupleRecycler.containsKey(id)) {
			//log.debug("tuple recycler already exists: "+id);
			return;
		}
		tupleRecycler.put(id, new Recycler<Tuple>(tuple,
				conf.getDragonRecyclerTupleCapacity(),
				conf.getDragonRecyclerTupleExpansion(),
				conf.getDragonRecyclerTupleCompact()));
	}
	
	public Recycler<Tuple> getTupleRecycler(String id) {
		return tupleRecycler.get(id);
	}
	
	public Recycler<NetworkTask> getNetworkTaskRecycler(){
		return networkTaskRecycler;
	}	

}
