package dragon.topology;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Config;
import dragon.network.NodeContext;


/**
 * @author aaron
 *
 */
public class DragonTopology implements Serializable {
	private static final long serialVersionUID = 5759609228559061827L;
	
	/**
	 * 
	 */
	private static Logger log = LogManager.getLogger(DragonTopology.class);
	
	/**
	 * 
	 */
	private HashMap<String,SpoutDeclarer> spoutMap;
	
	/**
	 * 
	 */
	private HashMap<String,BoltDeclarer> boltMap;
	
	/**
	 * 
	 */
	private SourceComponentMap topology;
	
	/**
	 * 
	 */
	private HashMap<String,HashMap<String,HashMap<String,GroupingsSet>>> sourceStreamComponentMap;
	
	/**
	 * 
	 */
	private ComponentEmbedding embedding;
	
	/**
	 * 
	 */
	private ReverseComponentEmbedding reverseEmbedding;
	
	/**
	 * 
	 */
	public DragonTopology() {
		topology=new SourceComponentMap();
		sourceStreamComponentMap = new HashMap<>();
	}
	
	/**
	 * @param fromComponentId
	 * @param toComponentId
	 * @param hashMap
	 */
	public void add(String fromComponentId, String toComponentId, StreamMap hashMap) {
		if(!topology.containsKey(fromComponentId)) {
			topology.put(fromComponentId,new DestComponentMap());
		}
		if(!sourceStreamComponentMap.containsKey(fromComponentId)) {
			sourceStreamComponentMap.put(fromComponentId,new HashMap<String,HashMap<String,GroupingsSet>>());
		}
		DestComponentMap destComponentMap = topology.get(fromComponentId);
		
		if(!destComponentMap.containsKey(toComponentId)) {
			destComponentMap.put(toComponentId, new StreamMap());
		}
		StreamMap streamMap = destComponentMap.get(toComponentId);
		for(String streamId : hashMap.keySet()) {
			log.debug("connecting ["+fromComponentId+"] to ["+toComponentId+"] on stream["+streamId+"]");
			if(!streamMap.containsKey(streamId)) {
				streamMap.put(streamId,new GroupingsSet());
			}
			GroupingsSet groupingsSet = streamMap.get(streamId);
			groupingsSet.addAll(hashMap.get(streamId));
			
			if(!sourceStreamComponentMap.get(fromComponentId).containsKey(streamId)) {
				sourceStreamComponentMap.get(fromComponentId).put(streamId,new HashMap<String,GroupingsSet>());
			}
			if(!sourceStreamComponentMap.get(fromComponentId).get(streamId).containsKey(toComponentId)) {
				sourceStreamComponentMap.get(fromComponentId).get(streamId).put(toComponentId,new GroupingsSet());
			}
			sourceStreamComponentMap.get(fromComponentId).get(streamId).get(toComponentId).addAll(hashMap.get(streamId));
		}
	}
	
	/**
	 * @param algo
	 * @param context
	 * @param config
	 */
	public void embedTopology(IEmbeddingAlgo algo, NodeContext context, Config config) {
		log.debug("Using the embedding algorithm " + algo.getClass().getCanonicalName());
		embedding = algo.generateEmbedding(this, context, config);
		reverseEmbedding = embedding.getReverseComponentEmbedding();
	}
	
	/**
	 * @param componentId
	 * @return
	 */
	public DestComponentMap getDestComponentMap(String componentId){
		return topology.get(componentId);
	}
	
	/**
	 * @param fromComponentId
	 * @param toComponentId
	 * @return
	 */
	public StreamMap getStreamMap(String fromComponentId, String toComponentId){
		return getDestComponentMap(fromComponentId).get(toComponentId);
	}
	
	/**
	 * @param fromComponentId
	 * @param toComponentId
	 * @param streamId
	 * @return
	 */
	public GroupingsSet getGroupingsSet(String fromComponentId, String toComponentId, String streamId){
		return getStreamMap(fromComponentId,toComponentId).get(streamId);
	}
	
	/**
	 * 
	 * @param fromComponentId
	 * @param streamId
	 * @return
	 */
	public HashMap<String,GroupingsSet> getComponentDestSet(String fromComponentId, String streamId){
		return sourceStreamComponentMap.get(fromComponentId).get(streamId);
	}


	/**
	 * @param spoutMap
	 */
	public void setSpoutMap(HashMap<String, SpoutDeclarer> spoutMap) {
		this.spoutMap=spoutMap;		
	}


	/**
	 * @param boltMap
	 */
	public void setBoltMap(HashMap<String, BoltDeclarer> boltMap) {
		this.boltMap=boltMap;
	}

	/**
	 * @return
	 */
	public HashMap<String, SpoutDeclarer> getSpoutMap() {
		return spoutMap;
	}

	/**
	 * @return
	 */
	public HashMap<String, BoltDeclarer> getBoltMap() {
		return boltMap;
	}

	/**
	 * @return
	 */
	public SourceComponentMap getTopology() {
		return topology;
	}

	/**
	 * @return
	 */
	public ComponentEmbedding getEmbedding() {
		return embedding;
	}

	/**
	 * @return
	 */
	public ReverseComponentEmbedding getReverseEmbedding() {
		return reverseEmbedding;
	}
}
