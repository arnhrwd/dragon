package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.TopologyErrorMessage;
import dragon.network.messages.service.TopologySubmittedMessage;
import dragon.network.messages.service.RunFailedMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarFileMessage;
import dragon.network.messages.node.PrepareTopologyMessage;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.JarFileMessage;
import dragon.network.messages.service.JarFileStoredMessage;
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
			case JARFILE:
				JarFileMessage jf = (JarFileMessage) command;
				if(node.getLocalClusters().containsKey(jf.topologyName)){
					TopologyErrorMessage r = new TopologyErrorMessage(jf.topologyName,"topology exists");
					r.setMessageId(jf.getMessageId());
					node.getComms().sendServiceMessage(r);
				} else {
					log.debug("storing topology ["+jf.topologyName+"]");
					if(!node.storeJarFile(jf.topologyName,jf.topologyJar)) {
						RunFailedMessage r = new RunFailedMessage(jf.topologyName,"could not store the topology jar");
						r.setMessageId(jf.getMessageId());
						node.getComms().sendServiceMessage(r);
						continue;
					}
					if(!node.loadJarFile(jf.topologyName)) {
						RunFailedMessage r = new RunFailedMessage(jf.topologyName,"could not load the topology jar");
						r.setMessageId(jf.getMessageId());
						node.getComms().sendServiceMessage(r);				
						continue;
					}
					
					JarFileStoredMessage r = new JarFileStoredMessage(jf.topologyName);
					r.setMessageId(jf.getMessageId());
					node.getComms().sendServiceMessage(r);
				}
				break;
			case RUN_TOPOLOGY:
				RunTopologyMessage scommand = (RunTopologyMessage) command;
				if(node.getLocalClusters().containsKey(scommand.topologyName)){
					TopologyErrorMessage r = new TopologyErrorMessage(scommand.topologyName,"topology exists");
					r.setMessageId(scommand.getMessageId());
					node.getComms().sendServiceMessage(r);
				} else {
					
					LocalCluster cluster=new LocalCluster(node);
					cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology, false);
					node.getRouter().submitTopology(scommand.topologyName, scommand.dragonTopology);
					node.getLocalClusters().put(scommand.topologyName, cluster);
					node.createStartupTopology(scommand.topologyName);
					boolean hit=false;
					for(NodeDescriptor desc : scommand.dragonTopology.getReverseEmbedding().keySet()) {
						if(!desc.equals(node.getComms().getMyNodeDescriptor())) {
							hit=true;
							NodeMessage preparejarfile = new PrepareJarFileMessage(scommand.topologyName,node.readJarFile(scommand.topologyName));
							node.getComms().sendNodeMessage(desc, preparejarfile);
						}
					}
					if(!hit) {
						node.getLocalClusters().get(scommand.topologyName).openAll();
						TopologySubmittedMessage r = new TopologySubmittedMessage(scommand.topologyName);
						r.setMessageId(scommand.getMessageId());
						node.getComms().sendServiceMessage(r);
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
						log.debug("cm is null");
						MetricsErrorMessage r = new MetricsErrorMessage("unknown topology or there are no samples available yet");
						r.setMessageId(command.getMessageId());
						node.getComms().sendServiceMessage(r);
					}
				} else {
					log.debug("metrics are not enabled");
					MetricsErrorMessage r = new MetricsErrorMessage("metrics are not enabled in dragon.properties for this node");
					r.setMessageId(command.getMessageId());
					node.getComms().sendServiceMessage(r);
				}
				break;
			case TERMINATE_TOPOLOGY:
			{
				TerminateTopologyMessage tt = (TerminateTopologyMessage) command;
				
			}
			break;
			default:
			}
		}
	}
}
