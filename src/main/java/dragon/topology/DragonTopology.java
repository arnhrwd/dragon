package dragon.topology;

import java.util.HashMap;
import java.util.HashSet;

import dragon.grouping.CustomStreamGrouping;

public class DragonTopology {
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
			if(!toComponent.containsKey(streamId)) {
				toComponent.put(streamId,new HashSet<CustomStreamGrouping>());
			}
			HashSet<CustomStreamGrouping> stream = toComponent.get(streamId);
			stream.addAll(hashMap.get(streamId));
		}
		
		
	}
}
