package dragon.topology;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;

public class TopologyBuilder {
	private static Log log = LogFactory.getLog(TopologyBuilder.class);
	private HashMap<String,SpoutDeclarer> spoutMap;
	private HashMap<String,BoltDeclarer> boltMap;
	
	public TopologyBuilder() {
		spoutMap=new HashMap<String,SpoutDeclarer>();
		boltMap=new HashMap<String,BoltDeclarer>();
	}

	public SpoutDeclarer setSpout(String spoutName, Spout spout, int parallelismHint) {
		if(spoutMap.containsKey(spoutName)||boltMap.containsKey(spoutName)) {
			throw new RuntimeException("bolt and spout names must be collectively unique: ["+spoutName+"] has already been set");
		}
		SpoutDeclarer spoutDeclarer = new SpoutDeclarer(spout,parallelismHint);
		spoutMap.put(spoutName,spoutDeclarer);
		return spoutDeclarer;
	}
	
	public BoltDeclarer setBolt(String boltName, Bolt bolt, int parallelismHint) {
		if(spoutMap.containsKey(boltName)||boltMap.containsKey(boltName)) {
			throw new RuntimeException("bolt and spout names must be collectively unique: ["+boltName+"] has already been set");
		}
		BoltDeclarer boltDeclarer = new BoltDeclarer(bolt,parallelismHint);
		boltMap.put(boltName, boltDeclarer);
		return boltDeclarer;
	}
	
	public DragonTopology createTopology() {
		DragonTopology topology=new DragonTopology();
		for(String spoutId : spoutMap.keySet()) {
			for(String boltId : boltMap.keySet()) {
				if(boltMap.get(boltId).groupings.containsKey(spoutId)) {
					// spoutId sends to boltId
					log.debug("connecting spout["+spoutId+"] to bolt["+boltId+"]");
					topology.add(spoutId,boltId,boltMap.get(boltId).groupings.get(spoutId));
				}
			}
		}
		for(String fromBoltId : boltMap.keySet()) {
			for(String toBoltId : boltMap.keySet()) {
				if(boltMap.get(toBoltId).groupings.containsKey(fromBoltId)) {
					log.debug("connecting bolt["+fromBoltId+"] to bolt["+toBoltId+"]");
					topology.add(fromBoltId, toBoltId, boltMap.get(toBoltId).groupings.get(fromBoltId));
				}
			}
		}
		topology.setSpoutMap(spoutMap);
		topology.setBoltMap(boltMap);
		return topology;
	}
}
