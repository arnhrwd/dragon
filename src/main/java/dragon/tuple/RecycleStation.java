package dragon.tuple;

import java.util.HashMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.Config;
import dragon.Constants;

public class RecycleStation {
	@SuppressWarnings("unused")
	private Logger log = LogManager.getLogger(RecycleStation.class);
	private static RecycleStation recycleStation=null;
	private final Config conf;
	
	public static void instanceInit(Config conf) {
		recycleStation=new RecycleStation(conf);
	}
	
	public static RecycleStation getInstance() {
		return recycleStation;
	}
	
	private final HashMap<String,Recycler<Tuple>> tupleRecycler;
	private final Recycler<NetworkTask> networkTaskRecycler;
	
	public RecycleStation(Config conf) {
		this.conf=conf;
		tupleRecycler=new HashMap<String,Recycler<Tuple>>();
		networkTaskRecycler=new Recycler<NetworkTask>(new NetworkTask(),
				conf.getDragonRecyclerTaskCapacity(),
				conf.getDragonRecyclerTaskExpansion(),
				conf.getDragonRecyclerTaskCompact());
		createTupleRecycler(new Tuple(new Fields(Constants.SYSTEM_TUPLE_FIELDS)));
	}
	
	public void createTupleRecycler(Tuple tuple) {
		String id=tuple.getFields().getFieldNamesAsString();
		if(tupleRecycler.containsKey(id)) {
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
