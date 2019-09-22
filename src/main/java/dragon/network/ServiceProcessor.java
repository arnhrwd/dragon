package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.Config;
import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.TopologyRunningMessage;
import dragon.network.messages.service.UploadJarFailedMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.messages.node.NodeMessage;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.UploadJarMessage;
import dragon.network.messages.service.UploadJarSuccessMessage;
import dragon.network.messages.service.GetMetricsErrorMessage;
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
			ServiceMessage command;
			try {
				command = node.getComms().receiveServiceMessage();
			} catch (InterruptedException e) {
				log.info("interrupted");
				break;
			}
			switch(command.getType()){
			case UPLOAD_JAR:
				UploadJarMessage jf = (UploadJarMessage) command;
				if(node.getLocalClusters().containsKey(jf.topologyName)){
					UploadJarFailedMessage r = new UploadJarFailedMessage(jf.topologyName,"topology exists");
					r.setMessageId(jf.getMessageId());
					node.getComms().sendServiceMessage(r);
				} else {
					log.debug("storing topology ["+jf.topologyName+"]");
					if(!node.storeJarFile(jf.topologyName,jf.topologyJar)) {
						UploadJarFailedMessage r = new UploadJarFailedMessage(jf.topologyName,"could not store the topology jar");
						r.setMessageId(jf.getMessageId());
						node.getComms().sendServiceMessage(r);
						continue;
					}
					if(!node.loadJarFile(jf.topologyName)) {
						UploadJarFailedMessage r = new UploadJarFailedMessage(jf.topologyName,"could not load the topology jar");
						r.setMessageId(jf.getMessageId());
						node.getComms().sendServiceMessage(r);				
						continue;
					}
					
					UploadJarSuccessMessage r = new UploadJarSuccessMessage(jf.topologyName);
					r.setMessageId(jf.getMessageId());
					node.getComms().sendServiceMessage(r);
				}
				break;
			case RUN_TOPOLOGY:
				RunTopologyMessage scommand = (RunTopologyMessage) command;
				if(node.getLocalClusters().containsKey(scommand.topologyName)){
					RunTopologyErrorMessage r = new RunTopologyErrorMessage(scommand.topologyName,"topology exists");
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
							NodeMessage preparejarfile = new PrepareJarMessage(scommand.topologyName,node.readJarFile(scommand.topologyName));
							preparejarfile.setMessageId(scommand.getMessageId());
							node.getComms().sendNodeMessage(desc, preparejarfile);
						}
					}
					if(!hit) {
						node.getLocalClusters().get(scommand.topologyName).openAll();
						TopologyRunningMessage r = new TopologyRunningMessage(scommand.topologyName);
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
				if((Boolean)node.getConf().getDragonMetricsEnabled()){
					ComponentMetricMap cm = node.getMetrics(gm.topologyId);
					if(cm!=null){
						MetricsMessage r = new MetricsMessage(cm);
						r.setMessageId(command.getMessageId());
						node.getComms().sendServiceMessage(r);
					} else {
						log.debug("cm is null");
						GetMetricsErrorMessage r = new GetMetricsErrorMessage("unknown topology or there are no samples available yet");
						r.setMessageId(command.getMessageId());
						node.getComms().sendServiceMessage(r);
					}
				} else {
					log.debug("metrics are not enabled");
					GetMetricsErrorMessage r = new GetMetricsErrorMessage("metrics are not enabled in dragon.properties for this node");
					r.setMessageId(command.getMessageId());
					node.getComms().sendServiceMessage(r);
				}
				break;
			case TERMINATE_TOPOLOGY:
			{
				TerminateTopologyMessage tt = (TerminateTopologyMessage) command;
				if(!node.getLocalClusters().containsKey(tt.topologyId)){
					TerminateTopologyErrorMessage r = new TerminateTopologyErrorMessage(tt.topologyId,"topology does not exist");
					r.setMessageId(tt.getMessageId());
					node.getComms().sendServiceMessage(r);
				} else {
					LocalCluster localCluster = node.getLocalClusters().get(tt.topologyId);
					localCluster.setTerminateMessageId(tt.getMessageId());
					localCluster.setShouldTerminate();
				}
			}
			break;
			default:
			}
		}
	}
}
