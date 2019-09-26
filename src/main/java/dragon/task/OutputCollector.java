package dragon.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;

import dragon.LocalCluster;

import dragon.topology.base.Collector;
import dragon.topology.base.Component;


import dragon.tuple.Tuple;

public class OutputCollector extends Collector {
	private static Log log = LogFactory.getLog(OutputCollector.class);
	
	public OutputCollector(LocalCluster localCluster,Component component) {
		super(component,localCluster,(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE));
	}
	
	@Deprecated
	public void ack(Tuple tuple) {
		
	}
	
	
}
