package dragon.topology;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.grouping.CustomStreamGrouping;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Fields;

public class BoltDeclarer extends Declarer {
	private Log log = LogFactory.getLog(BoltDeclarer.class);
	IRichBolt bolt;
	
	public BoltDeclarer(String name, int parallelismHint) {
		super(name, parallelismHint);
	}
	
	public BoltDeclarer(String name, IRichBolt bolt, int parallelismHint) {
		super(name, parallelismHint);
		this.bolt=bolt;
	}
	
	BoltDeclarer shuffleGrouping(String componentId) {
		return this;
	}
	
	BoltDeclarer shuffleGrouping(String componentId, String streamId) {
		return this;
	}
	
	BoltDeclarer allGrouping(String componentId) {
		return this;
	}
	
	BoltDeclarer allGrouping(String componentId, String streamId) {
		return this;
	}
	
	BoltDeclarer fieldGrouping(String componentId, Fields fields) {
		return this;
	}
	
	BoltDeclarer customGrouping(String componentId, CustomStreamGrouping customStreamGrouping) {
		return this;
	}

}
