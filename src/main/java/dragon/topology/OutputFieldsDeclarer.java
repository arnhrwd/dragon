package dragon.topology;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Constants;
import dragon.LocalCluster;
import dragon.tuple.Fields;

/**
 * @author aaron
 *
 */
public class OutputFieldsDeclarer implements Serializable {
	private static final long serialVersionUID = -3989587286666639746L;
	private static Logger log = LogManager.getLogger(OutputFieldsDeclarer.class);
	
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
	 * Declare the fields for the default stream.
	 * @param fields
	 */
	public void declare(Fields fields) {
		declare(Constants.DEFAULT_STREAM,fields);
	}
	
	/**
	 * Declare the fields for the default stream. If direct
	 * is true then also declare the fields for direct
	 * emit on the default stream.
	 * @param direct
	 * @param fields
	 */
	public void declare(boolean direct,Fields fields) {
		declare(Constants.DEFAULT_STREAM,fields);
		if(direct) {
			if(directStreamFields.containsKey(Constants.DEFAULT_STREAM)) {
				log.warn("stream declared more than once: "+Constants.DEFAULT_STREAM);
			}
			directStreamFields.put(Constants.DEFAULT_STREAM,fields);
		}
	}
	
	/**
	 * Declare the fields on the given stream id.
	 * @param streamId
	 * @param fields
	 */
	public void declare(String streamId,Fields fields) {
		if(streamFields.containsKey(streamId)) {
			log.warn("stream declared more than once: "+streamId);
		}
		streamFields.put(streamId, fields);
		setFieldsForGrouping(streamId, fields);
	}
	
	/**
	 * Declare the fields on the given stream id.
	 * @param streamId
	 * @param fields
	 */
	public void declareStream(String streamId,Fields fields) {
		declare(streamId,fields);
	}
	
	/**
	 * If direct is false then declare the fields on the given stream id.
	 * If direct is true then declare the fields on the stream id for direct emit
	 * use only. This method may be called once to declare the stream for regular
	 * emit using the grouping assigned to the stream, and again to declare the
	 * stream for direct emit usage.
	 * 
	 * @param streamId
	 * @param direct
	 * @param fields
	 */
	public void declareStream(String streamId,boolean direct,Fields fields) {
		if(!direct){
			declare(streamId,fields);
		} else {
			if(directStreamFields.containsKey(streamId)) {
				log.warn("stream declared more than once: "+streamId);
			}
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
	
	public Set<String> getStreams() {
		HashSet<String> streams = new HashSet<>();
		streams.addAll(directStreamFields.keySet());
		streams.addAll(streamFields.keySet());
		return streams;
	}
	
}
