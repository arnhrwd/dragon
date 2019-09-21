package dragon.spout;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;


import dragon.topology.base.Collector;

import dragon.topology.base.Spout;

public class SpoutOutputCollector extends Collector {
	private static Log log = LogFactory.getLog(SpoutOutputCollector.class);
	
	
	public SpoutOutputCollector(LocalCluster localCluster,Spout spout) {
		super(spout,localCluster,(Integer)localCluster.getConf().getDragonOutputBufferSize());

	}
	
	
}
