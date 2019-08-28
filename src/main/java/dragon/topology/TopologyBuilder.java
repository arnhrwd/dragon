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
		return null;
	}
}
