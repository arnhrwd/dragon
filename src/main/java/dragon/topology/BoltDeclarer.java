package dragon.topology;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Constants;
import dragon.grouping.AbstractGrouping;
import dragon.grouping.AllGrouping;
import dragon.grouping.DirectGrouping;
import dragon.grouping.FieldGrouping;
import dragon.grouping.ShuffleGrouping;
import dragon.topology.base.Bolt;

import dragon.tuple.Fields;

public class BoltDeclarer extends Declarer {
	private static final long serialVersionUID = 4947955477005135498L;
	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(BoltDeclarer.class);
	private Bolt bolt;
	
	// the components that this bolt listens to
	public HashMap<String,StreamMap> groupings;
	
	public Bolt getBolt() {
		return bolt;
	}
	
	@Override
	public BoltDeclarer setNumTasks(int numTasks) {
		super.setNumTasks(numTasks);
		return this;
	}
	
	public BoltDeclarer(String name, int parallelismHint) {
		super(name, parallelismHint);
		groupings=new HashMap<String,StreamMap>();
	}
	
	public BoltDeclarer(String name, Bolt bolt, int parallelismHint) {
		super(name, parallelismHint);
		groupings=new HashMap<String,StreamMap>();
		this.bolt=bolt;
	}
	
	private void put(String componentId,AbstractGrouping grouping) {
		put(componentId,Constants.DEFAULT_STREAM,grouping);
	}
	
	private void put(String componentId,String streamId,AbstractGrouping grouping) {
		if(!groupings.containsKey(componentId)) {
			groupings.put(componentId, new StreamMap());
			put(componentId,Constants.SYSTEM_STREAM_ID,new AllGrouping());
		}
		HashMap<String,GroupingsSet> map = groupings.get(componentId);
		if(!map.containsKey(streamId)) {
			map.put(streamId, new GroupingsSet());
		}
		GroupingsSet hs = map.get(streamId);
		hs.add(grouping);
	}
	
	public BoltDeclarer shuffleGrouping(String componentId) {
		put(componentId,new ShuffleGrouping());
		return this;
	}
	
	public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
		put(componentId,streamId,new ShuffleGrouping());
		return this;
	}
	
	public BoltDeclarer allGrouping(String componentId) {
		put(componentId,new AllGrouping());
		return this;
	}
	
	public BoltDeclarer allGrouping(String componentId, String streamId) {
		put(componentId,streamId,new AllGrouping());
		return this;
	}
	
	public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
		put(componentId,new FieldGrouping(fields));
		return this;
	}
	
	public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
		put(componentId,streamId,new FieldGrouping(fields));
		return this;
	}
	
	public BoltDeclarer customGrouping(String componentId, AbstractGrouping customStreamGrouping) {
		put(componentId,customStreamGrouping);
		return this;
	}
	
	public BoltDeclarer customGrouping(String componentId, String streamId, AbstractGrouping customStreamGrouping) {
		put(componentId,streamId,customStreamGrouping);
		return this;
	}
	
	public BoltDeclarer directGrouping(String componentId) {
		put(componentId,new DirectGrouping());
		return this;
	}
	
	public BoltDeclarer directGrouping(String componentId, String streamId){
		put(componentId,streamId,new DirectGrouping());
		return this;
	}

}
