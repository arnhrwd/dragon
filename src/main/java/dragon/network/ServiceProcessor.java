package dragon.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.TopologyExistsMessage;
import dragon.network.messages.service.TopologySubmittedMessage;
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
					node.getComms().sendServiceMessage(new TopologyExistsMessage(scommand.topologyName));
				} else {
					LocalCluster cluster=new LocalCluster();
					cluster.submitTopology(scommand.topologyName, scommand.conf, scommand.dragonTopology);
					node.getLocalClusters().put(scommand.topologyName, cluster);
					node.getComms().sendServiceMessage(new TopologySubmittedMessage(scommand.topologyName));
				}
				break;
			case GET_NODE_CONTEXT:
				node.getComms().sendServiceMessage(new NodeContextMessage(node.getNodeProcessor().getContext()));
				break;
			default:
			}
		}
	}
}
