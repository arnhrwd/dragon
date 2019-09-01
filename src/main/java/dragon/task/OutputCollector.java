package dragon.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;

import dragon.LocalCluster;

import dragon.topology.base.Collector;

import dragon.topology.base.IRichBolt;

import dragon.tuple.Tuple;

public class OutputCollector extends Collector {
	private Log log = LogFactory.getLog(OutputCollector.class);
	
	
	public OutputCollector(LocalCluster localCluster,IRichBolt iRichBolt) {
		super(iRichBolt,localCluster,(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE));
	}
	

	public void ack(Tuple tuple) {
		
	}
	
	
}
