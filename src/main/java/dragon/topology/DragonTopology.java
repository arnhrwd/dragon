package dragon.topology;

import java.io.Serializable;
import java.util.HashMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DragonTopology implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8549081242975660772L;
	private static Log log = LogFactory.getLog(DragonTopology.class);
	public HashMap<String,SpoutDeclarer> spoutMap;
	public HashMap<String,BoltDeclarer> boltMap;
	
	public SourceComponentMap topology;
	
	public DragonTopology() {
		topology=new SourceComponentMap();
	}
	

	public void add(String fromComponentId, String toComponentId, StreamMap hashMap) {
		if(!topology.containsKey(fromComponentId)) {
			topology.put(fromComponentId,new DestComponentMap());
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
			log.debug(groupingsSet);
		}
		
		
	}
	
	public DestComponentMap getDestComponentMap(String componentId){
		return topology.get(componentId);
	}
	
	public StreamMap getStreamMap(String fromComponentId, String toComponentId){
		return getDestComponentMap(fromComponentId).get(toComponentId);
	}
	
	public GroupingsSet getGroupingsSet(String fromComponentId, String toComponentId, String streamId){
		return getStreamMap(fromComponentId,toComponentId).get(streamId);
	}


	public void setSpoutMap(HashMap<String, SpoutDeclarer> spoutMap) {
		this.spoutMap=spoutMap;		
	}


	public void setBoltMap(HashMap<String, BoltDeclarer> boltMap) {
		this.boltMap=boltMap;
	}
}
