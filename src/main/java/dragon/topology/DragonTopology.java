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
	private static Logger log = LogManager.getLogger(DragonTopology.class);
	
	/**
	 * The user supplied name of the topology
	 */
	private String topologyId;
	
	/**
	 * component id -> component's declarer.
	 */
	private HashMap<String,SpoutDeclarer> spoutMap;
	
	/**
	 * component id -> component's declarer.
	 */
	private HashMap<String,BoltDeclarer> boltMap;
	
	/**
	 * source component id ->
	 * 	destination component id ->
	 *   stream id ->
	 *    set of groupings
	 */
	private SourceComponentMap topology;
	
	/**
	 * source component id ->
	 *  stream id ->
	 *   destination component id ->
	 *    set of groupings
	 */
	private HashMap<String,HashMap<String,HashMap<String,GroupingsSet>>> sourceStreamComponentMap;
	
	/**
	 * component id ->
	 *  task index ->
	 *   node
	 */
	private ComponentEmbedding embedding;
	
	/**
	 * node ->
	 * 	component id ->
	 * 	 task index set
	 */
	private ReverseComponentEmbedding reverseEmbedding;
	
	/**
	 * task id ->
	 *  component instance
	 */
	private HashMap<Integer,ComponentInstance> taskIdComponentInstanceMap;
	
	/**
	 * component id ->
	 *  task index ->
	 *   task id
	 */
	private HashMap<String,HashMap<Integer,Integer>> componentIndexIdMap;
	
	
	/**
	 * Constructor
	 */
	public DragonTopology(HashMap<String, SpoutDeclarer> spoutMap,
			HashMap<String, BoltDeclarer> boltMap) {
		this.spoutMap=spoutMap;
		this.boltMap=boltMap;
		topology=new SourceComponentMap();
		sourceStreamComponentMap = new HashMap<>();
		taskIdComponentInstanceMap = new HashMap<>();
		componentIndexIdMap = new HashMap<>();
		for(String spoutId : spoutMap.keySet()) {
			componentIndexIdMap.put(spoutId,new HashMap<Integer,Integer>());
			/*
			 * Allocate unique taskIds to the spout instances.
			 */
			for(int taskIndex=0;taskIndex<spoutMap.get(spoutId).getNumTasks();taskIndex++) {
				int taskId = taskIdComponentInstanceMap.size();
				taskIdComponentInstanceMap.put(taskId,new ComponentInstance(spoutId,taskIndex));
				componentIndexIdMap.get(spoutId).put(taskIndex,taskId);
			}
			
			
			/*
			 * Add mappings from this spout to all bolts that listen to it.
			 */
			for(String boltId : boltMap.keySet()) {
				if(boltMap.get(boltId).groupings.containsKey(spoutId)) {
					// spoutId sends to boltId
					log.debug("connecting spout["+spoutId+"] to bolt["+boltId+"]");
					add(spoutId,boltId,boltMap.get(boltId).groupings.get(spoutId));
				}
			}
		}
		for(String fromBoltId : boltMap.keySet()) {
			componentIndexIdMap.put(fromBoltId,new HashMap<Integer,Integer>());
			/*
			 * Allocate unique taskIds to the bolt instances.
			 */
			for(int taskIndex=0;taskIndex<boltMap.get(fromBoltId).getNumTasks();taskIndex++) {
				int taskId = taskIdComponentInstanceMap.size();
				taskIdComponentInstanceMap.put(taskId,new ComponentInstance(fromBoltId,taskIndex));
				componentIndexIdMap.get(fromBoltId).put(taskIndex,taskId);
			}
			
			/*
			 * Add mappings from this bolt to all bolts that listen to it.
			 */
			for(String toBoltId : boltMap.keySet()) {
				if(boltMap.get(toBoltId).groupings.containsKey(fromBoltId)) {
					log.debug("connecting bolt["+fromBoltId+"] to bolt["+toBoltId+"]");
					add(fromBoltId, toBoltId, boltMap.get(toBoltId).groupings.get(fromBoltId));
				}
			}
		}
	}
	
	/**
	 * Create various mappings that support efficient topology operations
	 * throughout the system, especially in places like {@link dragon.LocalCluster} and 
	 * {@link dragon.topology.base.Collector}.
	 * @param fromComponentId
	 * @param toComponentId
	 * @param hashMap
	 */
	public void add(String fromComponentId, String toComponentId, StreamMap hashMap) {
		
		/*
		 * Create various mappings that support efficient topology operations.
		 */
		
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
	
	/**
	 * 
	 * @param topologyId
	 */
	public void setTopologyId(String topologyId) {
		this.topologyId=topologyId;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getTopologyId() {
		return topologyId;
	}

	/**
	 * 
	 * @return
	 */
	public HashMap<String, HashMap<Integer, Integer>> getComponentIndexIdMap() {
		return componentIndexIdMap;
	}

}
