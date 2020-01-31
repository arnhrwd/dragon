package dragon.spout;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.LocalCluster;


import dragon.topology.base.Collector;

import dragon.topology.base.Spout;

public class SpoutOutputCollector extends Collector {
	@SuppressWarnings("unused")
	private static Logger log = LogManager.getLogger(SpoutOutputCollector.class);
	
	
	public SpoutOutputCollector(LocalCluster localCluster,Spout spout) {
		super(spout,localCluster,(Integer)localCluster.getConf().getDragonOutputBufferSize());

	}
	
	
}
