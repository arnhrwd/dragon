package dragon.task;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import dragon.Config;

import dragon.LocalCluster;

import dragon.topology.base.Collector;
import dragon.topology.base.Component;


import dragon.tuple.Tuple;

public class OutputCollector extends Collector {
	@SuppressWarnings("unused")
	private final static Logger log = LogManager.getLogger(OutputCollector.class);
	
	public OutputCollector(LocalCluster localCluster,Component component) {
		super(component,localCluster,(Integer)localCluster.getConf().get(Config.DRAGON_OUTPUT_BUFFER_SIZE));
	}
	
	@Deprecated
	public void ack(Tuple tuple) {
		
	}
	
	
}
