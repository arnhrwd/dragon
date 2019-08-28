package dragon.topology;

import dragon.topology.base.IRichBolt;
import dragon.topology.base.IRichSpout;

public class TopologyBuilder {

	public SpoutDeclarer setSpout(String spoutName, IRichSpout spout, int parallelismHint) {
		return null;
	}
	
	public BoltDeclarer setBolt(String spoutName, IRichBolt spout, int parallelismHint) {
		return null;
	}
}
