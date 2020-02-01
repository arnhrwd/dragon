package dragon.spout;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.LocalCluster;


import dragon.topology.base.Collector;

import dragon.topology.base.Spout;

/**
 * @author aaron
 *
 */
public class SpoutOutputCollector extends Collector {
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(SpoutOutputCollector.class);
	
	
	/**
	 * @param localCluster
	 * @param spout
	 */
	public SpoutOutputCollector(LocalCluster localCluster,Spout spout) {
		super(spout,localCluster,(Integer)localCluster.getConf().getDragonOutputBufferSize());

	}
	
	
}
