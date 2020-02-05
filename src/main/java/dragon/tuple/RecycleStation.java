package dragon.tuple;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Config;
import dragon.Constants;

/**
 * @author aaron
 *
 */
public class RecycleStation {
	@SuppressWarnings("unused")
	private Logger log = LogManager.getLogger(RecycleStation.class);
	
	/**
	 * 
	 */
	private static RecycleStation recycleStation=null;
	
	/**
	 * 
	 */
	private final Config conf;
	
	/**
	 * @param conf
	 */
	public static void instanceInit(Config conf) {
		recycleStation=new RecycleStation(conf);
	}
	
	/**
	 * @return
	 */
	public static RecycleStation getInstance() {
		return recycleStation;
	}
	
	/**
	 * 
	 */
	private final HashMap<String,Recycler<Tuple>> tupleRecycler;
	
	/**
	 * 
	 */
	private final Recycler<NetworkTask> networkTaskRecycler;
	
	/**
	 * @param conf
	 */
	public RecycleStation(Config conf) {
		this.conf=conf;
		tupleRecycler=new HashMap<String,Recycler<Tuple>>();
		networkTaskRecycler=new Recycler<NetworkTask>(new NetworkTask(),
				conf.getDragonRecyclerTaskCapacity(),
				conf.getDragonRecyclerTaskExpansion(),
				conf.getDragonRecyclerTaskCompact());
		createTupleRecycler(new Tuple(new Fields(Constants.SYSTEM_TUPLE_FIELDS)));
	}
	
	/**
	 * @param tuple
	 */
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
	
	/**
	 * @param id
	 * @return
	 */
	public Recycler<Tuple> getTupleRecycler(String id) {
		return tupleRecycler.get(id);
	}
	
	/**
	 * @return
	 */
	public Recycler<NetworkTask> getNetworkTaskRecycler(){
		return networkTaskRecycler;
	}	

}
