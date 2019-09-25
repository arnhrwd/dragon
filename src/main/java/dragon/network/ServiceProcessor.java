package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.metrics.ComponentMetricMap;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TerminateTopologyErrorMessage;
import dragon.network.messages.service.TerminateTopologyMessage;
import dragon.network.messages.service.TopologyRunningMessage;
import dragon.network.messages.service.UploadJarFailedMessage;
import dragon.network.messages.service.RunTopologyErrorMessage;
import dragon.network.comms.DragonCommsException;
import dragon.network.messages.node.PrepareJarMessage;
import dragon.network.messages.node.StopTopologyMessage;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.UploadJarMessage;
import dragon.network.messages.service.UploadJarSuccessMessage;
import dragon.network.operations.RunTopologyGroupOperation;
import dragon.network.operations.TerminateTopologyGroupOperation;
import dragon.topology.DragonTopology;
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
			case UPLOAD_JAR:{
				UploadJarMessage jf = (UploadJarMessage) command;
				if(node.getLocalClusters().containsKey(jf.topologyName)){
					try {
						node.getComms().sendServiceMessage(new UploadJarFailedMessage(jf.topologyName,"topology exists"),jf);
					} catch (DragonCommsException e) {
						// ignore
					}
				} else {
					log.info("storing topology ["+jf.topologyName+"]");
					if(!node.storeJarFile(jf.topologyName,jf.topologyJar)) {
						try {
							node.getComms().sendServiceMessage(new UploadJarFailedMessage(jf.topologyName,"could not store the topology jar"),jf);
						} catch (DragonCommsException e) {
							// ignore
						}
						continue;
					}
					if(!node.loadJarFile(jf.topologyName)) {
						try {
							node.getComms().sendServiceMessage(new UploadJarFailedMessage(jf.topologyName,"could not load the topology jar"),jf);
						} catch (DragonCommsException e) {
							// ignore
						}				
						continue;
					}
					try {
						node.getComms().sendServiceMessage(new UploadJarSuccessMessage(jf.topologyName),jf);
					} catch (DragonCommsException e) {
						// ignore
					}
				}
				break;
			}
			case RUN_TOPOLOGY:{
				RunTopologyMessage scommand = (RunTopologyMessage) command;
				if(node.getLocalClusters().containsKey(scommand.topologyName)){
					try {
						node.getComms().sendServiceMessage(new RunTopologyErrorMessage(scommand.topologyName,"topology exists"),scommand);
					} catch (DragonCommsException e) {
						// ignore
					}
				} else {
					
					RunTopologyGroupOperation rtgc = new RunTopologyGroupOperation(scommand,node);
					for(NodeDescriptor desc : scommand.dragonTopology.getReverseEmbedding().keySet()) {
						rtgc.add(desc);
					}
					node.register(rtgc);
					rtgc.initiate(node.getComms());
					
//					LocalCluster cluster=new LocalCluster(node);
//					cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology, false);
//					node.getRouter().submitTopology(scommand.topologyName, scommand.dragonTopology);
//					node.getLocalClusters().put(scommand.topologyName, cluster);
					// the jar is already prepared for this node
					rtgc.receiveSuccess(node.getComms(), node.getComms().getMyNodeDescriptor());
				}
				break;	
			}
			case GET_NODE_CONTEXT:{
				try {
					node.getComms().sendServiceMessage(
							new NodeContextMessage(node.getNodeProcessor().getContext()),command);
				} catch (DragonCommsException e) {
					// ignore
				}
				break;
			}
			case GET_METRICS:{
				GetMetricsMessage gm = (GetMetricsMessage) command;
				if((Boolean)node.getConf().getDragonMetricsEnabled()){
					ComponentMetricMap cm = node.getMetrics(gm.topologyId);
					if(cm!=null){
						try {
							node.getComms().sendServiceMessage(new MetricsMessage(cm),command);
						} catch (DragonCommsException e) {
							// ignore
						}
					} else {
						try {
							node.getComms().sendServiceMessage(new GetMetricsErrorMessage("unknown topology or there are no samples available yet"),command);
						} catch (DragonCommsException e) {
							// ignore
						}
					}
				} else {
					log.warn("metrics are not enabled");
					try {
						node.getComms().sendServiceMessage(new GetMetricsErrorMessage("metrics are not enabled in dragon.yaml for this node"),command);
					} catch (DragonCommsException e) {
						// ignore
					}
				}
				break;
			}
			case TERMINATE_TOPOLOGY:{
				TerminateTopologyMessage tt = (TerminateTopologyMessage) command;
				if(!node.getLocalClusters().containsKey(tt.topologyId)){
					try {
						node.getComms().sendServiceMessage(new TerminateTopologyErrorMessage(tt.topologyId,"topology does not exist"),command);
					} catch (DragonCommsException e) {
						// ignore
					}
				} else {
					DragonTopology dragonTopology = node.getLocalClusters().get(tt.topologyId).getTopology();
					TerminateTopologyGroupOperation ttgo = new TerminateTopologyGroupOperation(tt);
					for(NodeDescriptor desc : dragonTopology.getReverseEmbedding().keySet()) {
						ttgo.add(desc);
					}
					node.register(ttgo);
					ttgo.initiate(node.getComms());
					node.stopTopology(tt.topologyId, ttgo);
					
				}
				break;
			}
			default:
			}
		}
	}
}
