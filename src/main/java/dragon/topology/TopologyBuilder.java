package dragon.topology;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;

/**
 * Used to declare spouts and bolts, and finally to create a topology,
 * providing a {@link DragonTopology} object. Spouts and bolts are components
 * of the topology.
 * @author aaron
 * @see setSpout
 * @see setBolt
 * @see createTopology
 *
 */
public class TopologyBuilder {
	private static Logger log = LogManager.getLogger(TopologyBuilder.class);
	
	/**
	 * Map of declared spouts from their component ID
	 * to their declarer.
	 */
	private HashMap<String,SpoutDeclarer> spoutMap;
	
	/**
	 *  Map of declared bolts from their component ID
	 *  to their declarer.
	 */
	private HashMap<String,BoltDeclarer> boltMap;
	
	/**
	 * Initialize the topology builder.
	 */
	public TopologyBuilder() {
		spoutMap=new HashMap<String,SpoutDeclarer>();
		boltMap=new HashMap<String,BoltDeclarer>();
	}

	/**
	 * Declare a spout component.
	 * @param spoutName is a user provided name for the spout, it must be unique over
	 * all components (spouts and bolts).
	 * @param spout the object for the spout, containing its user defined code.
	 * @param parallelismHint the amount of parallelism (i.e. threads) that should be
	 * allocated to the instances of this spout. No more than this number of threads will
	 * be allocated.
	 * @return a spout declarer object
	 * @see #setBolt(String, Bolt, int)
	 * @see SpoutDeclarer
	 */
	public SpoutDeclarer setSpout(String spoutName, Spout spout, int parallelismHint) {
		if(spoutMap.containsKey(spoutName)||boltMap.containsKey(spoutName)) {
			throw new RuntimeException("bolt and spout names must be collectively unique: ["+spoutName+"] has already been set");
		}
		SpoutDeclarer spoutDeclarer = new SpoutDeclarer(spout,parallelismHint);
		spoutMap.put(spoutName,spoutDeclarer);
		return spoutDeclarer;
	}
	
	/**
	 * Declare a bolt component.
	 * @param boltName is a user provide name for the bolt, it must be unique over
	 * all components (spouts and bolts).
	 * @param bolt the object for the bolt, containing its user defined code.
	 * @param parallelismHint the amount of parallelism (i.e. threads) that should be
	 * allocated to the instances of this bolt. No more than this number of threads will
	 * be allocated.
	 * @return a bolt declarer object
	 * @see #setSpout(String, Spout, int)
	 * @see BoltDeclarer
	 */
	public BoltDeclarer setBolt(String boltName, Bolt bolt, int parallelismHint) {
		if(spoutMap.containsKey(boltName)||boltMap.containsKey(boltName)) {
			throw new RuntimeException("bolt and spout names must be collectively unique: ["+boltName+"] has already been set");
		}
		BoltDeclarer boltDeclarer = new BoltDeclarer(bolt,parallelismHint);
		boltMap.put(boltName, boltDeclarer);
		return boltDeclarer;
	}
	
	/**
	 * A DragonTopology can be submitted to run locally or to a Dragon network
	 * using {@link dragon.DragonSubmitter}.
	 * @return a DragonTopology
	 */
	public DragonTopology createTopology() {
		DragonTopology topology=new DragonTopology(spoutMap,boltMap);
		return topology;
	}
}
