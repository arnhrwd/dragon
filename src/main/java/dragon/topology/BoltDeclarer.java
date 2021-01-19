package dragon.topology;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.Constants;
import dragon.grouping.AbstractGrouping;
import dragon.grouping.AllGrouping;
import dragon.grouping.DirectGrouping;
import dragon.grouping.FieldGrouping;
import dragon.grouping.ShuffleGrouping;
import dragon.topology.base.Bolt;
import dragon.tuple.Fields;

/**
 * The bolt declarer allows you to declare the components that this
 * bolt listens to, including the <i>grouping</i> that defines which
 * instances of this bolt will receive tuples for the particular 
 * declaration.
 * @author aaron
 * @see dragon.tuple.Tuple
 */
public class BoltDeclarer extends Declarer {
	private static final long serialVersionUID = 4947955477005135498L;
	@SuppressWarnings("unused")
	private static final Logger log = LogManager.getLogger(BoltDeclarer.class);
	
	/**
	 * 
	 */
	private Bolt bolt;
	
	/**
	 * the components that this bolt listens to
	 */
	public HashMap<String,StreamMap> groupings;
	
	/**
	 * @return
	 */
	public Bolt getBolt() {
		return bolt;
	}
	
	/* (non-Javadoc)
	 * @see dragon.topology.Declarer#setNumTasks(int)
	 */
	@Override
	public BoltDeclarer setNumTasks(int numTasks) {
		super.setNumTasks(numTasks);
		return this;
	}
	
	/**
	 * @param parallelismHint
	 */
	public BoltDeclarer(int parallelismHint) {
		super(parallelismHint);
		groupings=new HashMap<String,StreamMap>();
	}
	
	/**
	 * @param bolt
	 * @param parallelismHint
	 */
	public BoltDeclarer(Bolt bolt, int parallelismHint) {
		super(parallelismHint);
		groupings=new HashMap<String,StreamMap>();
		this.bolt=bolt;
	}
	
	/**
	 * @param componentId
	 * @param grouping
	 */
	private void put(String componentId,AbstractGrouping grouping) {
		put(componentId,Constants.DEFAULT_STREAM,grouping);
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @param grouping
	 */
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
	
	/**
	 * @param componentId
	 * @return
	 */
	public BoltDeclarer shuffleGrouping(String componentId) {
		put(componentId,new ShuffleGrouping());
		return this;
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @return
	 */
	public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
		put(componentId,streamId,new ShuffleGrouping());
		return this;
	}
	
	/**
	 * @param componentId
	 * @return
	 */
	public BoltDeclarer allGrouping(String componentId) {
		put(componentId,new AllGrouping());
		return this;
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @return
	 */
	public BoltDeclarer allGrouping(String componentId, String streamId) {
		put(componentId,streamId,new AllGrouping());
		return this;
	}
	
	/**
	 * @param componentId
	 * @param fields
	 * @return
	 */
	public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
		put(componentId,new FieldGrouping(fields));
		return this;
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @param fields
	 * @return
	 */
	public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
		put(componentId,streamId,new FieldGrouping(fields));
		return this;
	}
	
	/**
	 * @param componentId
	 * @param customStreamGrouping
	 * @return
	 */
	public BoltDeclarer customGrouping(String componentId, AbstractGrouping customStreamGrouping) {
		put(componentId,customStreamGrouping);
		return this;
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @param customStreamGrouping
	 * @return
	 */
	public BoltDeclarer customGrouping(String componentId, String streamId, AbstractGrouping customStreamGrouping) {
		put(componentId,streamId,customStreamGrouping);
		return this;
	}
	
	/**
	 * @param componentId
	 * @return
	 */
	public BoltDeclarer directGrouping(String componentId) {
		put(componentId,new DirectGrouping());
		return this;
	}
	
	/**
	 * @param componentId
	 * @param streamId
	 * @return
	 */
	public BoltDeclarer directGrouping(String componentId, String streamId){
		put(componentId,streamId,new DirectGrouping());
		return this;
	}

}
