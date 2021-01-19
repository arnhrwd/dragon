package dragon.topology;

import java.util.HashMap;

import dragon.Constants;
import dragon.LocalCluster;
import dragon.tuple.Fields;

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
	public final LocalCluster localCluster;
	
	/**
	 * 
	 */
	public final String componentId;
	
	/**
	 * 
	 */
	public OutputFieldsDeclarer(LocalCluster localCluster,String componentId) {
		this.localCluster=localCluster;
		this.componentId=componentId;
		streamFields = new HashMap<String,Fields>();
		directStreamFields = new HashMap<String,Fields>();
		declare(Constants.SYSTEM_STREAM_ID,new Fields(Constants.SYSTEM_TUPLE_FIELDS));
	}
	
	private void setFieldsForGrouping(String streamId, Fields fields) {
		if(!streamId.equals(Constants.SYSTEM_STREAM_ID)) {
			localCluster.getTopology().getComponentDestSet(componentId, streamId).forEach((component2Id,groupingSet)->{
				groupingSet.forEach((grouping)-> {
					grouping.setSupportedFields(fields);
				});
			});
		}
	}
	
	/**
	 * @param fields
	 */
	public void declare(Fields fields) {
		declare(Constants.DEFAULT_STREAM,fields);
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
		setFieldsForGrouping(streamId, fields);
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
