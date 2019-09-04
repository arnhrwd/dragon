package dragon.topology;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.grouping.CustomStreamGrouping;

public class DragonTopology implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8549081242975660772L;
	private static Log log = LogFactory.getLog(DragonTopology.class);
	public HashMap<String,SpoutDeclarer> spoutMap;
	public HashMap<String,BoltDeclarer> boltMap;
	
	public HashMap<String,HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>>> topology;
	
	public DragonTopology() {
		topology=new HashMap<String,HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>>>();
	}
	

	public void add(String fromComponentId, String toComponentId, HashMap<String, HashSet<CustomStreamGrouping>> hashMap) {
		if(!topology.containsKey(fromComponentId)) {
			topology.put(fromComponentId,new HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>>());
		}
		HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>> fromComponent =
				topology.get(fromComponentId);
		
		if(!fromComponent.containsKey(toComponentId)) {
			fromComponent.put(toComponentId, new HashMap<String,HashSet<CustomStreamGrouping>>());
		}
		HashMap<String,HashSet<CustomStreamGrouping>> toComponent = fromComponent.get(toComponentId);
		for(String streamId : hashMap.keySet()) {
			log.debug("connecting ["+fromComponentId+"] to ["+toComponentId+"] on stream["+streamId+"]");
			if(!toComponent.containsKey(streamId)) {
				toComponent.put(streamId,new HashSet<CustomStreamGrouping>());
			}
			HashSet<CustomStreamGrouping> stream = toComponent.get(streamId);
			stream.addAll(hashMap.get(streamId));
			log.debug(stream);
		}
		
		
	}
	
	public HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>> getFromComponent(String componentId){
		return topology.get(componentId);
	}
	
	public HashMap<String,HashSet<CustomStreamGrouping>> getFromToComponent(String fromComponentId, String toComponentId){
		return getFromComponent(fromComponentId).get(toComponentId);
	}
	
	public HashSet<CustomStreamGrouping> getFromToStream(String fromComponentId, String toComponentId, String streamId){
		return getFromToComponent(fromComponentId,toComponentId).get(streamId);
	}


	public void setSpoutMap(HashMap<String, SpoutDeclarer> spoutMap) {
		this.spoutMap=spoutMap;		
	}


	public void setBoltMap(HashMap<String, BoltDeclarer> boltMap) {
		this.boltMap=boltMap;
	}
}
