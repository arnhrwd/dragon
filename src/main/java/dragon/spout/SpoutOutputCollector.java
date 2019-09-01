package dragon.spout;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;


import dragon.topology.base.Collector;
import dragon.topology.base.IRichSpout;

public class SpoutOutputCollector extends Collector {
	private Log log = LogFactory.getLog(SpoutOutputCollector.class);
	
	
	public SpoutOutputCollector(LocalCluster localCluster,IRichSpout iRichSpout) {
		super(iRichSpout,localCluster,(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE));

	}
	
	
}
