package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.network.messages.service.RunTopology;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyExists;
import dragon.network.messages.service.TopologySubmitted;

public class ServiceProcessor extends Thread {
	private static Log log = LogFactory.getLog(ServiceProcessor.class);
	private boolean shouldTerminate=false;
	private Node node;
	public ServiceProcessor(Node node) {
		this.node=node;
		log.debug("starting service processor");
		start();
	}
	public void run(){
		while(!shouldTerminate){
			ServiceMessage command = node.getComms().receiveServiceMessage();
			switch(command.getType()){
			case RUN_TOPOLOGY:
				RunTopology scommand = (RunTopology) command;
				if(node.getLocalClusters().containsKey(scommand.topologyName)){
					node.getComms().sendServiceMessage(new TopologyExists(scommand.topologyName));
				} else {
					LocalCluster cluster=new LocalCluster();
					cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology);
					node.getLocalClusters().put(scommand.topologyName, cluster);
					node.getComms().sendServiceMessage(new TopologySubmitted(scommand.topologyName));
				}
				break;
			default:
			}
		}
	}
}
