package dragon.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.network.Node;

public class Metrics extends Thread {
	private static Log log = LogFactory.getLog(Metrics.class);
	private Node node;
	
	private TopologyMetricMap samples;
	
	public Metrics(Node node){
		samples=new TopologyMetricMap((int)node.getConf().get("DRAGON_METRICS_SAMPLE_HISTORY"));
		this.node = node;
		start();
	}
	
	public ComponentMetricMap getMetrics(String topologyId){
		synchronized(samples){
			return samples.get(topologyId);
		}
	}
	
	@Override
	public void run(){
		while(!isInterrupted()){
			try {
				sleep((long)node.getConf().get("DRAGON_METRICS_SAMPLE_PERIOD_MS"));
			} catch (InterruptedException e) {
				log.info("shutting down");
			}
			synchronized(samples){
				for(String topologyId : node.getLocalClusters().keySet()){
					LocalCluster localCluster = node.getLocalClusters().get(topologyId);
					for(String componentId : localCluster.getSpouts().keySet()){
						for(Integer taskId : localCluster.getSpouts().get(componentId).keySet()){
							Sample sample = new Sample(localCluster.getSpouts().get(componentId).get(taskId));
							samples.put(topologyId, componentId, taskId, sample);
						}
					}
					for(String componentId : localCluster.getBolts().keySet()){
						for(Integer taskId : localCluster.getBolts().get(componentId).keySet()){
							Sample sample = new Sample(localCluster.getBolts().get(componentId).get(taskId));
							samples.put(topologyId, componentId, taskId, sample);
						}
					}
				}
			}
		}
	}

}
