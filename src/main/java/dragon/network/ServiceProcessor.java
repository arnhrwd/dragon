package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyExistsMessage;
import dragon.network.messages.service.RunFailedMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.MetricsErrorMessage;
import dragon.network.messages.service.MetricsMessage;
import dragon.network.messages.service.NodeContextMessage;

public class ServiceProcessor extends Thread {
	private static Log log = LogFactory.getLog(ServiceProcessor.class);
	private boolean shouldTerminate=false;
	private Node node;
	public ServiceProcessor(Node node) {
		this.node=node;
		log.debug("starting service processor");
		start();
	}
	
	@Override
	public void run(){
		while(!shouldTerminate){
			ServiceMessage command = node.getComms().receiveServiceMessage();
			switch(command.getType()){
			case RUN_TOPOLOGY:
				RunTopologyMessage scommand = (RunTopologyMessage) command;
				if(node.getLocalClusters().containsKey(scommand.topologyName)){
					TopologyExistsMessage r = new TopologyExistsMessage(scommand.topologyName);
					r.setMessageId(scommand.getMessageId());
					node.getComms().sendServiceMessage(r);
				} else {
					log.debug("storing topology ["+scommand.topologyName+"]");
					if(!node.storeJarFile(scommand.topologyName,scommand.topologyJar)) {
						RunFailedMessage r = new RunFailedMessage(scommand.topologyName,"could not store the topology jar");
						r.setMessageId(scommand.getMessageId());
						node.getComms().sendServiceMessage(r);
						continue;
					}
					if(!node.loadJarFile(scommand.topologyName)) {
						RunFailedMessage r = new RunFailedMessage(scommand.topologyName,"could not load the topology jar");
						r.setMessageId(scommand.getMessageId());
						node.getComms().sendServiceMessage(r);				
						continue;
					}
					LocalCluster cluster=new LocalCluster(node);
					cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology, false);
					node.getRouter().submitTopology(scommand.topologyName, scommand.dragonTopology);
					node.getLocalClusters().put(scommand.topologyName, cluster);
					node.createStartupTopology(scommand.topologyName);
					boolean hit=false;
					for(NodeDescriptor desc : scommand.dragonTopology.getReverseEmbedding().keySet()) {
						if(!desc.equals(node.getComms().getMyNodeDescriptor())) {
							hit=true;
							NodeMessage message = new PrepareTopologyMessage(scommand.topologyName,scommand.conf,scommand.dragonTopology,scommand.topologyJar);
							node.getComms().sendNodeMessage(desc, message);
						}
					}
					if(!hit) {
						node.getLocalClusters().get(scommand.topologyName).openAll();
					}
				}
				break;
			case GET_NODE_CONTEXT:
				{
				NodeContextMessage r = new NodeContextMessage(node.getNodeProcessor().getContext());
				r.setMessageId(command.getMessageId());
				node.getComms().sendServiceMessage(r);
				}
				break;
			case GET_METRICS:
				GetMetricsMessage gm = (GetMetricsMessage) command;
				if((Boolean)node.getConf().get(Config.DRAGON_METRICS_ENABLED)){
					ComponentMetricMap cm = node.getMetrics(gm.topologyId);
					if(cm!=null){
						MetricsMessage r = new MetricsMessage(cm);
						r.setMessageId(command.getMessageId());
						node.getComms().sendServiceMessage(r);
					} else {
						MetricsErrorMessage r = new MetricsErrorMessage();
						r.setMessageId(command.getMessageId());
						node.getComms().sendServiceMessage(r);
					}
				} else {
					MetricsErrorMessage r = new MetricsErrorMessage();
					r.setMessageId(command.getMessageId());
					node.getComms().sendServiceMessage(r);
				}
			default:
			}
		}
	}
}
