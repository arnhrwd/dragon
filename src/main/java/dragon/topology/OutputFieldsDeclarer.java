package dragon.topology;

import java.util.HashMap;

import dragon.Constants;
import dragon.tuple.Fields;
import dragon.tuple.RecycleStation;
import dragon.tuple.Tuple;

/**
 * @author aaron
 *
 */
public class OutputFieldsDeclarer {
	/**
	 * 
	 */
	public final HashMap<String,Fields> streamFields;
	
	/**
	 * 
	 */
	public final HashMap<String,Fields> directStreamFields;
	
	/**
	 * 
	 */
	public OutputFieldsDeclarer() {
		streamFields = new HashMap<String,Fields>();
		directStreamFields = new HashMap<String,Fields>();
		declare(Constants.SYSTEM_STREAM_ID,new Fields(Constants.SYSTEM_TUPLE_FIELDS));
	}
	
	/**
	 * @param fields
	 */
	public void declare(Fields fields) {
		declare(Constants.DEFAULT_STREAM,fields);
		RecycleStation.getInstance().createTupleRecycler(new Tuple(fields));
	}
	
	/**
	 * @param direct
	 * @param fields
	 */
	public void declare(boolean direct,Fields fields) {
		//if(direct==false){
			declare(Constants.DEFAULT_STREAM,fields);
		//} else {
			directStreamFields.put(Constants.DEFAULT_STREAM,fields);
		//}
	}
	
	/**
	 * @param streamId
	 * @param fields
	 */
	public void declare(String streamId,Fields fields) {
		streamFields.put(streamId, fields);
		RecycleStation.getInstance().createTupleRecycler(new Tuple(fields));
	}
	
	/**
	 * @param streamId
	 * @param fields
	 */
	public void declareStream(String streamId,Fields fields) {
		declare(streamId,fields);
	}
	
	/**
	 * @param streamId
	 * @param direct
	 * @param fields
	 */
	public void declareStream(String streamId,boolean direct,Fields fields) {
		if(!direct){
			declare(streamId,fields);
		} else {
			directStreamFields.put(streamId, fields);
			RecycleStation.getInstance().createTupleRecycler(new Tuple(fields));
		}
	}
	
	/**
	 * @param streamId
	 * @return
	 */
	public Fields getFields(String streamId) {
		return streamFields.get(streamId);
	}
	
	/**
	 * @param streamId
	 * @return
	 */
	public Fields getFieldsDirect(String streamId) {
		return directStreamFields.get(streamId);
	}
	
}
