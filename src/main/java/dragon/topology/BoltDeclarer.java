package dragon.topology;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Constants;
import dragon.grouping.AllGrouping;
import dragon.grouping.CustomStreamGrouping;
import dragon.grouping.FieldGrouping;
import dragon.grouping.ShuffleGrouping;
import dragon.topology.base.IRichBolt;
import dragon.tuple.Fields;

public class BoltDeclarer extends Declarer {
	private Log log = LogFactory.getLog(BoltDeclarer.class);
	private IRichBolt bolt;
	
	// the components that this bolt listens to
	public HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>> groupings;
	
	public IRichBolt getBolt() {
		return bolt;
	}
	
	public BoltDeclarer(String name, int parallelismHint) {
		super(name, parallelismHint);
		groupings=new HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>>();
	}
	
	public BoltDeclarer(String name, IRichBolt bolt, int parallelismHint) {
		super(name, parallelismHint);
		groupings=new HashMap<String,HashMap<String,HashSet<CustomStreamGrouping>>>();
		this.bolt=bolt;
	}
	
	private void put(String componentId,CustomStreamGrouping grouping) {
		put(componentId,Constants.DEFAULT_STREAM,grouping);
	}
	
	private void put(String componentId,String streamId,CustomStreamGrouping grouping) {
		if(!groupings.containsKey(componentId)) {
			groupings.put(componentId, new HashMap<String,HashSet<CustomStreamGrouping>>());
		}
		HashMap<String,HashSet<CustomStreamGrouping>> map = groupings.get(componentId);
		if(!map.containsKey(streamId)) {
			map.put(streamId, new HashSet<CustomStreamGrouping>());
		}
		HashSet<CustomStreamGrouping> hs = map.get(streamId);
		hs.add(grouping);
	}
	
	BoltDeclarer shuffleGrouping(String componentId) {
		put(componentId,new ShuffleGrouping());
		return this;
	}
	
	BoltDeclarer shuffleGrouping(String componentId, String streamId) {
		put(componentId,streamId,new ShuffleGrouping());
		return this;
	}
	
	BoltDeclarer allGrouping(String componentId) {
		put(componentId,new AllGrouping());
		return this;
	}
	
	BoltDeclarer allGrouping(String componentId, String streamId) {
		put(componentId,streamId,new AllGrouping());
		return this;
	}
	
	BoltDeclarer fieldGrouping(String componentId, Fields fields) {
		put(componentId,new FieldGrouping(fields));
		return this;
	}
	
	BoltDeclarer customGrouping(String componentId, CustomStreamGrouping customStreamGrouping) {
		put(componentId,customStreamGrouping);
		return this;
	}

}