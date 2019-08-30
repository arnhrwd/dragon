package dragon.topology;

import java.util.HashMap;

import dragon.topology.base.IRichBolt;
import dragon.topology.base.IRichSpout;

public class TopologyBuilder {
	
	HashMap<String,SpoutDeclarer> spoutMap;
	HashMap<String,BoltDeclarer> boltMap;
	
	public TopologyBuilder() {
		spoutMap=new HashMap<String,SpoutDeclarer>();
		boltMap=new HashMap<String,BoltDeclarer>();
	}

	public SpoutDeclarer setSpout(String spoutName, IRichSpout spout, int parallelismHint) {
		SpoutDeclarer spoutDeclarer = new SpoutDeclarer(spoutName,spout,parallelismHint);
		spoutMap.put(spoutName,spoutDeclarer);
		return spoutDeclarer;
	}
	
	public BoltDeclarer setBolt(String boltName, IRichBolt bolt, int parallelismHint) {
		BoltDeclarer boltDeclarer = new BoltDeclarer(boltName,bolt,parallelismHint);
		boltMap.put(boltName, boltDeclarer);
		return boltDeclarer;
	}
	
	public DragonTopology createTopology() {
		DragonTopology topology=new DragonTopology();
		for(String spoutId : spoutMap.keySet()) {
			for(String boltId : boltMap.keySet()) {
				if(boltMap.get(boltId).groupings.containsKey(spoutId)) {
					// spoutId sends to boltId
					topology.add(spoutId,boltId,boltMap.get(boltId).groupings.get(spoutId));
				}
			}
		}
		for(String fromBoltId : boltMap.keySet()) {
			for(String toBoltId : boltMap.keySet()) {
				if(boltMap.get(toBoltId).groupings.containsKey(fromBoltId)) {
					topology.add(fromBoltId, toBoltId, boltMap.get(toBoltId).groupings.get(fromBoltId));
				}
			}
		}
		return topology;
	}
}
